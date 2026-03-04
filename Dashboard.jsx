import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import * as d3 from "d3";

const API_BASE = window.__API_BASE__ || "http://localhost:8000";

// ── Color & style config ────────────────────────────────────
const ENTITY_COLORS = {
  Person: "#f59e0b",
  Company: "#3b82f6",
  Institution: "#10b981",
  Contract: "#ef4444",
  PoliticalParty: "#8b5cf6",
  Address: "#6b7280",
  BudgetItem: "#ec4899",
};

const SEVERITY_COLORS = {
  critical: "#dc2626",
  high: "#f97316",
  medium: "#eab308",
  low: "#6b7280",
};

const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 };

// ── Demo fallback data ───────────────────────────────────────
const DEMO_NODES = [
  { id: "P-001", name: "Petar Petrović", type: "Person", props: { current_role: "Direktor sektora" } },
  { id: "P-002", name: "Marija Petrović", type: "Person", props: { current_role: "Osnivač" } },
  { id: "P-003", name: "Nikola Jovanović", type: "Person", props: { current_role: "Član odbora" } },
  { id: "C-001", name: "TechSerb DOO", type: "Company", props: { status: "Aktivan" } },
  { id: "C-002", name: "DataLink DOO", type: "Company", props: { status: "Aktivan" } },
  { id: "C-003", name: "InfoSys PR", type: "Company", props: { status: "Aktivan" } },
  { id: "I-001", name: "Ministarstvo finansija", type: "Institution", props: {} },
  { id: "I-002", name: "Grad Beograd", type: "Institution", props: {} },
  { id: "CT-001", name: "IT infrastruktura - faza 1", type: "Contract", props: { value_rsd: 45000000 } },
  { id: "CT-002", name: "Softverska podrška", type: "Contract", props: { value_rsd: 12000000 } },
  { id: "CT-003", name: "Održavanje sistema", type: "Contract", props: { value_rsd: 8500000 } },
  { id: "PP-001", name: "Stranka napretka", type: "PoliticalParty", props: {} },
];

const DEMO_EDGES = [
  { source: "P-001", target: "I-001", relationship: "EMPLOYED_BY" },
  { source: "P-001", target: "P-002", relationship: "FAMILY_OF" },
  { source: "P-002", target: "C-001", relationship: "OWNS" },
  { source: "P-003", target: "C-002", relationship: "DIRECTS" },
  { source: "P-003", target: "C-003", relationship: "DIRECTS" },
  { source: "I-001", target: "CT-001", relationship: "AWARDED_CONTRACT" },
  { source: "I-002", target: "CT-002", relationship: "AWARDED_CONTRACT" },
  { source: "I-001", target: "CT-003", relationship: "AWARDED_CONTRACT" },
  { source: "C-001", target: "CT-001", relationship: "WON_CONTRACT" },
  { source: "C-001", target: "CT-003", relationship: "WON_CONTRACT" },
  { source: "C-002", target: "CT-002", relationship: "WON_CONTRACT" },
  { source: "P-001", target: "PP-001", relationship: "MEMBER_OF" },
  { source: "P-003", target: "PP-001", relationship: "MEMBER_OF" },
];

const DEMO_ALERTS = [
  {
    pattern_type: "conflict_of_interest", severity: "critical",
    description: "Petar Petrović (Ministarstvo finansija) — supruga Marija Petrović je osnivač TechSerb DOO koja je dobila ugovor od 45M RSD od istog ministarstva.",
    entities: ["P-001", "P-002", "C-001", "CT-001"],
    official_name: "Petar Petrović", company_name: "TechSerb DOO", contract_value: 45000000,
  },
  {
    pattern_type: "single_bidder", severity: "high",
    description: "Ugovor 'IT infrastruktura - faza 1' (45M RSD) — samo jedan ponuđač. TechSerb DOO je jedini učesnik.",
    entities: ["CT-001", "C-001"],
    contract_title: "IT infrastruktura - faza 1", value_rsd: 45000000,
  },
  {
    pattern_type: "contract_splitting", severity: "medium",
    description: "Dva ugovora za TechSerb DOO od Ministarstva finansija u kratkom periodu — ukupno 53.5M RSD.",
    entities: ["CT-001", "CT-003", "C-001", "I-001"],
    total_value: 53500000, num_contracts: 2,
  },
  {
    pattern_type: "revolving_door", severity: "medium",
    description: "Nikola Jovanović — direktor dve firme (DataLink, InfoSys) i član iste stranke kao Petar Petrović.",
    entities: ["P-003", "C-002", "C-003", "PP-001"],
    person_name: "Nikola Jovanović",
  },
];

const PATTERN_LABELS = {
  conflict_of_interest: "Sukob interesa",
  single_bidder: "Jedan ponuđač",
  contract_splitting: "Deljenje ugovora",
  revolving_door: "Rotirajuća vrata",
  ghost_employee: "Fantomski zaposleni",
  shell_company_cluster: "Shell kompanije",
  budget_self_allocation: "Samododeljivanje",
  political_donor_contract: "Donator→Ugovor",
};

const PATTERN_EXPLANATIONS = {
  conflict_of_interest: {
    title: "Sukob interesa",
    icon: "⚖",
    why: "Funkcioner koji direktno odlučuje o dodeli ugovora ima porodičnog člana koji je vlasnik ili direktor firme koja je dobila taj ugovor od iste institucije. Ovo je klasičan obrazac korupcije koji narušava princip nepristrasnosti u javnim nabavkama.",
    how: "Graf prati sledeći put:\n(Funkcioner) –[EMPLOYED_BY]→ (Institucija) –[AWARDED_CONTRACT]→ (Ugovor) ←[WON_CONTRACT]– (Firma) ←[OWNS|DIRECTS]– (Porodični član) –[FAMILY_OF]– (Funkcioner)\n\nSvi čvorovi u ovom putu moraju biti istovremeno prisutni da bi se uzorak aktivirao.",
    sources: ["APR — vlasništvo i direktorska mesta", "Portal javnih nabavki — ugovori i nosioci", "Evidencije funkcionera — zaposlenje u institucijama"],
    fields: [
      { key: "official_name", label: "Funkcioner" },
      { key: "official_role", label: "Pozicija" },
      { key: "institution", label: "Institucija" },
      { key: "family_member", label: "Porodični član" },
      { key: "company_name", label: "Firma" },
      { key: "contract_title", label: "Ugovor" },
      { key: "contract_value", label: "Vrednost ugovora" },
      { key: "award_date", label: "Datum dodele" },
    ],
  },
  ghost_employee: {
    title: "Fantomski zaposleni",
    icon: "👻",
    why: "Isto lice (prema normalizovanom imenu) pojavljuje se u platnom spisku dve ili više različitih institucija. Ovo može ukazivati na lažno zaposlenje, isplatu plata za nepostojećeg radnika, ili zloupotrebu evidencija.",
    how: "Graf traži dva čvora Person sa identičnim name_normalized vrednostima koji imaju različite person_id i koji su vezani EMPLOYED_BY odnosom za različite institucije:\n(P1:Person {name_normalized: X}) –[EMPLOYED_BY]→ (I1)\n(P2:Person {name_normalized: X}) –[EMPLOYED_BY]→ (I2)\ngde P1.person_id ≠ P2.person_id i I1 ≠ I2",
    sources: ["Evidencije javnih funkcionera — data.gov.rs", "Službeni glasnik — rešenja o postavljenju", "RIK — biračke i kadrovske evidencije"],
    fields: [
      { key: "name_1", label: "Ime (evidencija 1)" },
      { key: "institution_1", label: "Institucija 1" },
      { key: "name_2", label: "Ime (evidencija 2)" },
      { key: "institution_2", label: "Institucija 2" },
      { key: "normalized_name", label: "Normalizovano ime" },
    ],
  },
  shell_company_cluster: {
    title: "Klaster shell kompanija",
    icon: "🐚",
    why: "Tri ili više firmi registrovanih na istoj adresi kolektivno osvajaju javne ugovore. Ovo je čest mehanizam za rasipanje ugovora između povezanih firmi radi zaobilaženja pragova i obmanjivanja institucija o stvarnoj konkurenciji.",
    how: "Graf traži adresni čvor koji je povezan sa 3+ kompanija:\n(A:Address) ←[REGISTERED_AT]– (C1), (C2), (C3...)\ngde svaka kompanija ima bar jedan WON_CONTRACT odnos.\nSuma vrednosti svih ugovora se računa kao ukupna vrednost klastera.",
    sources: ["APR — registrovane adrese firmi", "Portal javnih nabavki — dodeljeni ugovori"],
    fields: [
      { key: "address", label: "Zajednička adresa" },
      { key: "city", label: "Grad" },
      { key: "num_companies", label: "Broj firmi" },
      { key: "num_contracts", label: "Broj ugovora" },
      { key: "total_value", label: "Ukupna vrednost" },
    ],
  },
  single_bidder: {
    title: "Ugovor sa jednim ponuđačem",
    icon: "1️⃣",
    why: "Javna nabavka je primila samo jednu ponudu. Iako ponekad opravdano, ovo drastično smanjuje konkurenciju i povećava rizik od dogovorenih nabavki. Posebno sumnjivo kada se ponavlja sa istom firmom ili institucijom.",
    how: "Graf traži ugovore gde:\n(I:Institution) –[AWARDED_CONTRACT]→ (CT:Contract {num_bidders: 1})\n(C:Company) –[WON_CONTRACT]→ (CT)\ngde ct.value_rsd ≥ prag (podrazumevano 1.000.000 RSD)",
    sources: ["Portal javnih nabavki — broj ponuda i pobednici"],
    fields: [
      { key: "contract_title", label: "Naziv ugovora" },
      { key: "value_rsd", label: "Vrednost" },
      { key: "award_date", label: "Datum dodele" },
      { key: "institution", label: "Naručilac" },
      { key: "winner", label: "Pobednik" },
      { key: "proc_type", label: "Vrsta nabavke" },
    ],
  },
  revolving_door: {
    title: "Rotirajuća vrata",
    icon: "🔄",
    why: "Bivši državni funkcioner ili regulator napustio je instituciju i direktno otišao da rukovodi privatnom firmom koja potom dobija ugovore od te iste institucije. Ovo je klasičan sukob interesa poznat kao 'revolving door' — lice koristi insajderska znanja i kontakte.",
    how: "Graf prati:\n(P:Person) –[EMPLOYED_BY {until: datum}]→ (I:Institution)\n(P) –[DIRECTS|OWNS {since: datum}]→ (C:Company)\ngde e2.since ≥ e1.until (otišao iz institucije PRE nego što je ušao u firmu)\nOPCIONALNO: (I) –[AWARDED_CONTRACT]→ (CT) ←[WON_CONTRACT]– (C)",
    sources: ["Službeni glasnik — rešenja o razrešenju", "APR — direktorska imenovanja", "Portal javnih nabavki — ugovori"],
    fields: [
      { key: "person_name", label: "Osoba" },
      { key: "former_institution", label: "Bivša institucija" },
      { key: "govt_role", label: "Bivša pozicija" },
      { key: "left_govt", label: "Datum odlaska" },
      { key: "company_name", label: "Nova firma" },
      { key: "company_role", label: "Nova pozicija" },
      { key: "joined_company", label: "Datum ulaska u firmu" },
      { key: "contracts_between", label: "Broj ugovora između" },
      { key: "total_contract_value", label: "Ukupna vrednost ugovora" },
    ],
  },
  budget_self_allocation: {
    title: "Samododeljivanje budžeta",
    icon: "💰",
    why: "Político ili funkcioner je odobrio budžetsku stavku, a ugovor finansiran iz te stavke dobila je firma u kojoj on/ona ima porodične ili vlasničke veze. Direktni sukob interesa na nivou budžetskog procesa.",
    how: "Graf traži:\n(P:Person) –[ALLOCATED_BY]– (B:BudgetItem) –[FUNDS]→ (CT:Contract)\n(C:Company) –[WON_CONTRACT]→ (CT)\ngde postoji put dužine 1-3 između P i C kroz FAMILY_OF, OWNS ili DIRECTS odnose",
    sources: ["Ministarstvo finansija — budžetske stavke", "Portal javnih nabavki — ugovori", "APR — vlasništvo firmi"],
    fields: [
      { key: "allocator", label: "Odobrio budžet" },
      { key: "budget_item", label: "Budžetska stavka" },
      { key: "amount", label: "Iznos" },
      { key: "contract_title", label: "Ugovor" },
      { key: "beneficiary_company", label: "Korisnik" },
    ],
  },
  contract_splitting: {
    title: "Deljenje ugovora",
    icon: "✂",
    why: "Ista firma dobija više ugovora od iste institucije u kratkom vremenskom periodu, pri čemu su svi ispod zakonskog praga za obaveznu međunarodnu licitaciju. Zbir vrednosti prelazi prag — klasičan način zaobilaženja procedura.",
    how: "Graf traži:\n(I:Institution) –[AWARDED_CONTRACT]→ (CT1, CT2... :Contract)\n(C:Company) –[WON_CONTRACT]→ (CT1, CT2...)\ngde: ct.value_rsd < prag AND ct.value_rsd > prag×0.5 AND count ≥ 2\nVremenski uslov: svi ugovori u roku od 90 dana jedan od drugog",
    sources: ["Portal javnih nabavki — hronologija ugovora"],
    fields: [
      { key: "institution", label: "Institucija" },
      { key: "company_name", label: "Firma" },
      { key: "num_contracts", label: "Broj ugovora" },
      { key: "total_value", label: "Ukupna vrednost" },
      { key: "first_date", label: "Prvi ugovor" },
      { key: "last_date", label: "Poslednji ugovor" },
    ],
  },
  political_donor_contract: {
    title: "Donator stranke → Ugovor",
    icon: "🤝",
    why: "Firma koja je finansirala političku stranku potom osvaja javne ugovore od institucija kojima rukovode članovi te stranke. Ovo je obrazac korupcije poznat kao 'pay-to-play' — donacija kao investicija u buduće ugovore.",
    how: "Graf prati:\n(C:Company) –[DONATED_TO]→ (PP:PoliticalParty)\n(C) –[WON_CONTRACT]→ (CT:Contract)\n(I:Institution) –[AWARDED_CONTRACT]→ (CT)\nOPCIONALNO: (P:Person) –[MEMBER_OF]→ (PP) AND (P) –[EMPLOYED_BY]→ (I)",
    sources: ["Agencija za borbu protiv korupcije — finansiranje stranaka", "APR — firmski donatori", "Portal javnih nabavki — ugovori"],
    fields: [
      { key: "donor_company", label: "Donator" },
      { key: "party_name", label: "Stranka" },
      { key: "donation_amount", label: "Iznos donacije" },
      { key: "contract_title", label: "Dobijeni ugovor" },
      { key: "contract_value", label: "Vrednost ugovora" },
      { key: "awarding_institution", label: "Institucija" },
      { key: "party_member_in_institution", label: "Član stranke u instituciji" },
    ],
  },
};

// ── Pattern Detail Modal ─────────────────────────────────────
function PatternDetailModal({ alert, onClose, onShowOnGraph }) {
  if (!alert) return null;
  const patternKey = alert.pattern_type;
  const exp = PATTERN_EXPLANATIONS[patternKey] || {};
  const color = SEVERITY_COLORS[alert.severity] || "#6b7280";

  const renderValue = (key) => {
    const val = alert[key];
    if (val == null || val === "") return null;
    if (key.includes("value") || key.includes("amount") || key.includes("total_value")) {
      return formatRSD(val) + " RSD";
    }
    return String(val);
  };

  return (
    <div style={{
      position: "fixed", inset: 0, zIndex: 1000,
      display: "flex", alignItems: "stretch", justifyContent: "flex-end",
      background: "rgba(0,0,0,0.6)", backdropFilter: "blur(2px)",
      animation: "fadeIn 0.15s ease-out",
    }} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div style={{
        width: "min(560px, 95vw)", background: "#0d1525",
        borderLeft: "1px solid #1e293b", overflowY: "auto",
        display: "flex", flexDirection: "column",
        animation: "slideIn 0.2s ease-out",
      }}>
        <style>{`
          @keyframes slideIn { from { transform: translateX(40px); opacity: 0 } to { transform: translateX(0); opacity: 1 } }
        `}</style>

        {/* Header */}
        <div style={{
          padding: "18px 20px 14px", borderBottom: "1px solid #1e293b",
          background: `linear-gradient(135deg, ${color}18, transparent)`,
          position: "sticky", top: 0, zIndex: 1,
          backdropFilter: "blur(8px)", backgroundColor: "#0d1525ee",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 22 }}>{exp.icon || "⚠"}</span>
              <div>
                <div style={{
                  fontSize: 16, fontWeight: 700, color: "#f8fafc",
                  letterSpacing: "-0.02em",
                }}>
                  {exp.title || PATTERN_LABELS[patternKey] || patternKey}
                </div>
                <div style={{ marginTop: 4 }}>
                  <RiskBadge level={alert.severity} />
                </div>
              </div>
            </div>
            <button onClick={onClose} style={{
              background: "#1e293b", border: "1px solid #334155",
              color: "#94a3b8", width: 28, height: 28, borderRadius: 6,
              cursor: "pointer", fontSize: 16, display: "flex",
              alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>×</button>
          </div>
        </div>

        <div style={{ padding: "20px", display: "flex", flexDirection: "column", gap: 20 }}>

          {/* Evidence fields */}
          <Section title="Detektovani entiteti" icon="🔍">
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {(exp.fields || []).map(({ key, label }) => {
                const val = renderValue(key);
                if (!val) return null;
                return (
                  <div key={key} style={{
                    display: "flex", gap: 10, padding: "6px 10px",
                    background: "#111827", borderRadius: 6,
                    alignItems: "baseline",
                  }}>
                    <span style={{
                      fontSize: 10, color: "#475569", minWidth: 150,
                      fontFamily: "'IBM Plex Mono', monospace", flexShrink: 0,
                    }}>{label}</span>
                    <span style={{
                      fontSize: 12, color: "#e2e8f0", fontWeight: 500,
                      wordBreak: "break-word",
                    }}>
                      {key.includes("value") || key.includes("amount") || key.includes("total_value")
                        ? <span style={{ color: "#f59e0b", fontFamily: "'IBM Plex Mono', monospace", fontWeight: 700 }}>{val}</span>
                        : val}
                    </span>
                  </div>
                );
              })}
              {/* Fallback: show any leftover fields not in template */}
              {!(exp.fields?.length) && Object.entries(alert)
                .filter(([k]) => !["pattern_type","severity","entities","description"].includes(k) && alert[k])
                .map(([k, v]) => (
                  <div key={k} style={{
                    display: "flex", gap: 10, padding: "6px 10px",
                    background: "#111827", borderRadius: 6, alignItems: "baseline",
                  }}>
                    <span style={{ fontSize: 10, color: "#475569", minWidth: 150, fontFamily: "'IBM Plex Mono', monospace" }}>{k}</span>
                    <span style={{ fontSize: 12, color: "#e2e8f0" }}>{String(v)}</span>
                  </div>
                ))}
            </div>
          </Section>

          {/* Why suspicious */}
          {exp.why && (
            <Section title="Zašto je sumnjivo" icon="⚠">
              <p style={{ fontSize: 13, lineHeight: 1.7, color: "#cbd5e1", margin: 0 }}>
                {exp.why}
              </p>
            </Section>
          )}

          {/* How detected */}
          {exp.how && (
            <Section title="Kako je detektovano" icon="◎">
              <pre style={{
                fontSize: 11, lineHeight: 1.7, color: "#94a3b8",
                fontFamily: "'IBM Plex Mono', monospace", margin: 0,
                whiteSpace: "pre-wrap", wordBreak: "break-word",
                background: "#060c18", padding: "12px 14px",
                borderRadius: 6, border: "1px solid #1e293b",
              }}>
                {exp.how}
              </pre>
            </Section>
          )}

          {/* Sources */}
          {exp.sources?.length > 0 && (
            <Section title="Korišćeni izvori podataka" icon="◈">
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {exp.sources.map((src, i) => (
                  <div key={i} style={{
                    display: "flex", alignItems: "center", gap: 8,
                    padding: "6px 10px", background: "#111827", borderRadius: 6,
                  }}>
                    <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#3b82f6", flexShrink: 0 }} />
                    <span style={{ fontSize: 12, color: "#94a3b8" }}>{src}</span>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {/* Raw data toggle */}
          <RawDataSection alert={alert} />

          {/* CTA */}
          <button onClick={onShowOnGraph} style={{
            padding: "10px 16px", borderRadius: 8,
            background: `linear-gradient(135deg, ${color}33, ${color}22)`,
            border: `1px solid ${color}66`,
            color: "#f8fafc", fontSize: 13, fontWeight: 600, cursor: "pointer",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
            transition: "all 0.15s",
          }}
            onMouseEnter={(e) => e.currentTarget.style.background = `linear-gradient(135deg, ${color}55, ${color}33)`}
            onMouseLeave={(e) => e.currentTarget.style.background = `linear-gradient(135deg, ${color}33, ${color}22)`}
          >
            <span>◎</span> Prikaži na grafu
          </button>
        </div>
      </div>
    </div>
  );
}

function Section({ title, icon, children }) {
  return (
    <div>
      <div style={{
        fontSize: 9, fontWeight: 700, textTransform: "uppercase",
        letterSpacing: "0.12em", color: "#475569", marginBottom: 10,
        fontFamily: "'IBM Plex Mono', monospace",
        display: "flex", alignItems: "center", gap: 6,
      }}>
        <span>{icon}</span> {title}
      </div>
      {children}
    </div>
  );
}

function RawDataSection({ alert }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button onClick={() => setOpen(o => !o)} style={{
        background: "none", border: "none", color: "#475569",
        fontSize: 10, fontFamily: "'IBM Plex Mono', monospace",
        cursor: "pointer", padding: 0, display: "flex", alignItems: "center", gap: 4,
      }}>
        <span style={{ transform: open ? "rotate(90deg)" : "none", display: "inline-block", transition: "transform 0.15s" }}>▶</span>
        Sirovi podaci (JSON)
      </button>
      {open && (
        <pre style={{
          marginTop: 8, fontSize: 10, color: "#64748b",
          fontFamily: "'IBM Plex Mono', monospace",
          background: "#060c18", padding: "12px 14px",
          borderRadius: 6, border: "1px solid #1e293b",
          overflowX: "auto", whiteSpace: "pre-wrap", wordBreak: "break-all",
        }}>
          {JSON.stringify(alert, null, 2)}
        </pre>
      )}
    </div>
  );
}

// ── API helpers ──────────────────────────────────────────────
async function apiFetch(path, options = {}) {
  try {
    const resp = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers: { "Content-Type": "application/json", ...options.headers },
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } catch (e) {
    console.warn(`API call failed: ${path}`, e.message);
    return null;
  }
}

function formatRSD(value) {
  if (!value && value !== 0) return "—";
  if (value >= 1e9) return `${(value / 1e9).toFixed(1)}B`;
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
  if (value >= 1e3) return `${(value / 1e3).toFixed(0)}K`;
  return value.toLocaleString("sr-RS");
}

// ── Loading spinner ──────────────────────────────────────────
function Spinner({ size = 20 }) {
  return (
    <div style={{
      width: size, height: size, border: "2px solid #334155",
      borderTop: "2px solid #3b82f6", borderRadius: "50%",
      animation: "spin 0.8s linear infinite",
    }} />
  );
}

// ── Force Graph Component ───────────────────────────────────
function ForceGraph({ nodes, edges, onNodeClick, highlightIds }) {
  const svgRef = useRef(null);
  const simRef = useRef(null);

  useEffect(() => {
    if (!svgRef.current || !nodes.length) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const width = svgRef.current.clientWidth;
    const height = svgRef.current.clientHeight;

    const g = svg.append("g");

    // Zoom
    const zoom = d3.zoom().scaleExtent([0.1, 6]).on("zoom", (e) => g.attr("transform", e.transform));
    svg.call(zoom);

    // Arrow markers
    const defs = svg.append("defs");
    defs.append("marker")
      .attr("id", "arrow-default")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 28)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", "#475569");

    // Drop shadow for highlighted nodes
    const filter = defs.append("filter").attr("id", "glow");
    filter.append("feGaussianBlur").attr("stdDeviation", "3").attr("result", "blur");
    filter.append("feMerge").selectAll("feMergeNode")
      .data(["blur", "SourceGraphic"]).join("feMergeNode")
      .attr("in", d => d);

    const nodeMap = new Map(nodes.map((n) => [n.id, { ...n }]));
    const simNodes = nodes.map((n) => ({ ...n }));
    const simEdges = edges
      .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      .map((e) => ({ ...e }));

    const sim = d3.forceSimulation(simNodes)
      .force("link", d3.forceLink(simEdges).id((d) => d.id).distance(130).strength(0.7))
      .force("charge", d3.forceManyBody().strength(-500))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(40))
      .force("x", d3.forceX(width / 2).strength(0.03))
      .force("y", d3.forceY(height / 2).strength(0.03));

    simRef.current = sim;

    // Edges
    const link = g.append("g").selectAll("line").data(simEdges).join("line")
      .attr("stroke", d => {
        if (d.relationship === "FAMILY_OF") return "#f59e0b44";
        if (d.relationship.includes("CONTRACT")) return "#ef444444";
        return "#334155";
      })
      .attr("stroke-width", d => d.relationship.includes("CONTRACT") ? 2 : 1.5)
      .attr("stroke-opacity", 0.6)
      .attr("stroke-dasharray", d => d.relationship === "FAMILY_OF" ? "4,4" : null)
      .attr("marker-end", "url(#arrow-default)");

    const linkLabel = g.append("g").selectAll("text").data(simEdges).join("text")
      .text((d) => d.relationship.replace(/_/g, " "))
      .attr("font-size", 7)
      .attr("fill", "#475569")
      .attr("text-anchor", "middle")
      .attr("font-family", "'IBM Plex Mono', 'JetBrains Mono', monospace")
      .attr("pointer-events", "none");

    // Nodes
    const node = g.append("g").selectAll("g").data(simNodes).join("g")
      .style("cursor", "pointer")
      .call(d3.drag()
        .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
        .on("end", (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on("click", (e, d) => onNodeClick?.(d));

    const isHighlighted = (d) => !highlightIds || highlightIds.has(d.id);

    node.append("circle")
      .attr("r", (d) => {
        if (d.type === "Contract") return 14;
        if (d.type === "Institution") return 20;
        if (d.type === "Company") return 17;
        return 16;
      })
      .attr("fill", (d) => ENTITY_COLORS[d.type] || "#6b7280")
      .attr("stroke", (d) => isHighlighted(d) && highlightIds ? "#fff" : "transparent")
      .attr("stroke-width", (d) => isHighlighted(d) && highlightIds ? 3 : 0)
      .attr("opacity", (d) => isHighlighted(d) ? 1 : 0.15)
      .attr("filter", (d) => isHighlighted(d) && highlightIds ? "url(#glow)" : null);

    node.append("text")
      .text((d) => d.name?.length > 20 ? d.name.slice(0, 18) + "…" : d.name)
      .attr("dy", 32)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", "#cbd5e1")
      .attr("font-family", "'DM Sans', 'Space Grotesk', sans-serif")
      .attr("opacity", (d) => isHighlighted(d) ? 1 : 0.15)
      .attr("pointer-events", "none");

    // Type icon
    const typeIcons = { Person: "👤", Company: "🏢", Institution: "🏛", Contract: "📄", PoliticalParty: "⚑", Address: "📍" };
    node.append("text")
      .text((d) => typeIcons[d.type] || "?")
      .attr("dy", 5)
      .attr("text-anchor", "middle")
      .attr("font-size", 12)
      .attr("pointer-events", "none");

    sim.on("tick", () => {
      link.attr("x1", (d) => d.source.x).attr("y1", (d) => d.source.y)
        .attr("x2", (d) => d.target.x).attr("y2", (d) => d.target.y);
      linkLabel.attr("x", (d) => (d.source.x + d.target.x) / 2)
        .attr("y", (d) => (d.source.y + d.target.y) / 2 - 4);
      node.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });

    // Zoom to fit after layout settles
    setTimeout(() => {
      svg.transition().duration(600).call(
        zoom.transform,
        d3.zoomIdentity.translate(width * 0.1, height * 0.1).scale(0.85)
      );
    }, 800);

    return () => sim.stop();
  }, [nodes, edges, highlightIds, onNodeClick]);

  return <svg ref={svgRef} style={{ width: "100%", height: "100%", background: "transparent" }} />;
}

// ── Risk Badge ──────────────────────────────────────────────
function RiskBadge({ level }) {
  const colors = {
    critical: { bg: "#dc262622", border: "#dc2626", text: "#fca5a5" },
    high: { bg: "#f9731622", border: "#f97316", text: "#fdba74" },
    medium: { bg: "#eab30822", border: "#eab308", text: "#fde047" },
    low: { bg: "#6b728022", border: "#6b7280", text: "#9ca3af" },
  };
  const c = colors[level] || colors.low;
  return (
    <span style={{
      fontSize: 9, padding: "2px 8px", borderRadius: 4,
      background: c.bg, border: `1px solid ${c.border}`,
      color: c.text, fontWeight: 700, textTransform: "uppercase",
      fontFamily: "'IBM Plex Mono', monospace", letterSpacing: "0.05em",
    }}>{level}</span>
  );
}

// ── Main Dashboard ──────────────────────────────────────────
export default function Dashboard() {
  const [activeTab, setActiveTab] = useState("graph");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [highlightIds, setHighlightIds] = useState(null);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [detailAlert, setDetailAlert] = useState(null);
  const [isDemo, setIsDemo] = useState(true);
  const [loading, setLoading] = useState(true);

  // Data state
  const [nodes, setNodes] = useState(DEMO_NODES);
  const [edges, setEdges] = useState(DEMO_EDGES);
  const [alerts, setAlerts] = useState(DEMO_ALERTS);
  const [stats, setStats] = useState(null);
  const [riskSummary, setRiskSummary] = useState(null);

  // Load data from API on mount
  useEffect(() => {
    let cancelled = false;
    async function init() {
      setLoading(true);

      // Try API stats
      const statsData = await apiFetch("/stats");
      if (!cancelled && statsData && (statsData.total_persons > 0 || statsData.total_companies > 0)) {
        setStats(statsData);
        setIsDemo(false);

        // Load detection results
        const detections = await apiFetch("/detect/all");
        if (!cancelled && detections) {
          setRiskSummary(detections.risk_summary);
          // Flatten alerts from all detectors
          const allAlerts = [];
          for (const [name, data] of Object.entries(detections.detections || {})) {
            for (const pattern of (data.patterns || [])) {
              allAlerts.push({ ...pattern, pattern_type: pattern.pattern_type || name });
            }
          }
          allAlerts.sort((a, b) => (SEVERITY_ORDER[a.severity] || 3) - (SEVERITY_ORDER[b.severity] || 3));
          if (allAlerts.length > 0) setAlerts(allAlerts);
        }
      } else {
        // Demo mode
        setStats({
          total_persons: DEMO_NODES.filter(n => n.type === "Person").length,
          total_companies: DEMO_NODES.filter(n => n.type === "Company").length,
          total_contracts: DEMO_NODES.filter(n => n.type === "Contract").length,
          total_institutions: DEMO_NODES.filter(n => n.type === "Institution").length,
          total_relationships: DEMO_EDGES.length,
        });
      }

      if (!cancelled) setLoading(false);
    }
    init();
    return () => { cancelled = true; };
  }, []);

  // Live search
  useEffect(() => {
    if (searchQuery.length < 2 || isDemo) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setIsSearching(true);
      const data = await apiFetch(`/search?q=${encodeURIComponent(searchQuery)}&limit=10`);
      if (data?.results) setSearchResults(data.results);
      setIsSearching(false);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery, isDemo]);

  // Graph exploration (on search result click)
  const exploreEntity = useCallback(async (id, type) => {
    if (isDemo) return;
    setLoading(true);
    const data = await apiFetch(`/graph/neighborhood?entity_id=${id}&entity_type=${type}&depth=2`);
    if (data?.nodes?.length) {
      setNodes(data.nodes);
      setEdges(data.edges || []);
      setActiveTab("graph");
      setSearchResults([]);
      setSearchQuery("");
    }
    setLoading(false);
  }, [isDemo]);

  const handleAlertClick = useCallback((alert) => {
    setDetailAlert(alert);
  }, []);

  const handleShowOnGraph = useCallback((alert) => {
    setDetailAlert(null);
    setSelectedAlert(alert);
    const entityIds = alert.entities ||
      [alert.official_id, alert.family_id, alert.company_mb, alert.contract_id,
       alert.person_id, alert.institution_id, alert.winner_mb].filter(Boolean);
    setHighlightIds(new Set(entityIds));
    setActiveTab("graph");
  }, []);

  const handleNodeClick = useCallback((node) => {
    setSelectedNode(node);
    const connected = new Set([node.id]);
    edges.forEach((e) => {
      const src = typeof e.source === "string" ? e.source : e.source?.id;
      const tgt = typeof e.target === "string" ? e.target : e.target?.id;
      if (src === node.id || tgt === node.id) {
        connected.add(src);
        connected.add(tgt);
      }
    });
    setHighlightIds(connected);
  }, [edges]);

  const clearSelection = () => {
    setSelectedNode(null);
    setSelectedAlert(null);
    setDetailAlert(null);
    setHighlightIds(null);
  };

  const filteredAlerts = useMemo(() =>
    alerts.filter((a) =>
      !searchQuery ||
      (a.description || "").toLowerCase().includes(searchQuery.toLowerCase()) ||
      (PATTERN_LABELS[a.pattern_type] || "").toLowerCase().includes(searchQuery.toLowerCase())
    ), [alerts, searchQuery]);

  const totalContractValue = useMemo(() => {
    return nodes
      .filter(n => n.type === "Contract")
      .reduce((sum, n) => sum + (n.props?.value_rsd || 0), 0);
  }, [nodes]);

  return (
    <>
    {detailAlert && (
      <PatternDetailModal
        alert={detailAlert}
        onClose={() => setDetailAlert(null)}
        onShowOnGraph={() => handleShowOnGraph(detailAlert)}
      />
    )}
    <div style={{
      width: "100vw", height: "100vh", background: "#080c15",
      color: "#e2e8f0", fontFamily: "'DM Sans', 'Space Grotesk', system-ui, sans-serif",
      display: "flex", flexDirection: "column", overflow: "hidden",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
        @keyframes spin { to { transform: rotate(360deg) } }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(6px) } to { opacity: 1; transform: translateY(0) } }
        @keyframes pulse { 0%, 100% { opacity: 1 } 50% { opacity: 0.5 } }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
      `}</style>

      {/* Header */}
      <header style={{
        padding: "10px 20px", borderBottom: "1px solid #151d2e",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        background: "linear-gradient(180deg, #0d1425 0%, #080c15 100%)",
        flexShrink: 0,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            width: 34, height: 34, borderRadius: 8,
            background: "linear-gradient(135deg, #dc2626, #f59e0b)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontWeight: 700, fontSize: 14, color: "#fff",
            fontFamily: "'IBM Plex Mono', monospace",
            boxShadow: "0 0 20px #dc262633",
          }}>ST</div>
          <div>
            <div style={{ fontWeight: 600, fontSize: 15, letterSpacing: "-0.02em" }}>
              Srpska Transparentnost
            </div>
            <div style={{
              fontSize: 10, color: "#475569", fontFamily: "'IBM Plex Mono', monospace",
              display: "flex", alignItems: "center", gap: 6,
            }}>
              <span>GRAPH INTELLIGENCE</span>
              {isDemo && (
                <span style={{
                  fontSize: 8, background: "#f5970b22", color: "#f59e0b",
                  padding: "1px 6px", borderRadius: 3, fontWeight: 600,
                }}>DEMO</span>
              )}
            </div>
          </div>
        </div>

        <div style={{ display: "flex", gap: 8, alignItems: "center", position: "relative" }}>
          <div style={{ position: "relative" }}>
            <input
              type="text"
              placeholder="Pretraži entitete..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{
                background: "#131b2e", border: "1px solid #1e293b", borderRadius: 6,
                padding: "6px 12px 6px 30px", color: "#e2e8f0", fontSize: 13, width: 240,
                outline: "none", fontFamily: "'DM Sans', sans-serif",
                transition: "border-color 0.2s",
              }}
              onFocus={(e) => e.target.style.borderColor = "#3b82f6"}
              onBlur={(e) => e.target.style.borderColor = "#1e293b"}
            />
            <span style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)", fontSize: 13, color: "#475569" }}>⌕</span>
            {isSearching && <div style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)" }}><Spinner size={14} /></div>}

            {/* Search results dropdown */}
            {searchResults.length > 0 && (
              <div style={{
                position: "absolute", top: "100%", left: 0, right: 0,
                background: "#131b2e", border: "1px solid #1e293b",
                borderRadius: 6, marginTop: 4, zIndex: 100,
                boxShadow: "0 8px 24px #00000066", maxHeight: 280, overflowY: "auto",
              }}>
                {searchResults.map((r, i) => (
                  <div key={i}
                    onClick={() => exploreEntity(r.id, r.type)}
                    style={{
                      padding: "8px 12px", cursor: "pointer",
                      borderBottom: i < searchResults.length - 1 ? "1px solid #1e293b" : "none",
                      transition: "background 0.1s",
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.background = "#1e293b"}
                    onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}
                  >
                    <div style={{ fontSize: 12, fontWeight: 500 }}>{r.name}</div>
                    <div style={{ fontSize: 10, color: ENTITY_COLORS[r.type] || "#64748b", fontFamily: "'IBM Plex Mono', monospace" }}>
                      {r.type} {r.role ? `• ${r.role}` : ""} {r.status ? `• ${r.status}` : ""}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={{
            background: "#131b2e", borderRadius: 6, display: "flex",
            border: "1px solid #1e293b", overflow: "hidden",
          }}>
            {[
              { key: "graph", label: "Graf", icon: "◎" },
              { key: "alerts", label: "Upozorenja", icon: "⚠" },
              { key: "data", label: "Podaci", icon: "◈" },
            ].map(({ key, label, icon }) => (
              <button key={key} onClick={() => setActiveTab(key)}
                style={{
                  padding: "6px 14px", fontSize: 11, fontWeight: 500,
                  background: activeTab === key ? "#1e293b" : "transparent",
                  color: activeTab === key ? "#f8fafc" : "#64748b",
                  border: "none", cursor: "pointer",
                  letterSpacing: "0.02em", fontFamily: "'IBM Plex Mono', monospace",
                  transition: "all 0.15s",
                  display: "flex", alignItems: "center", gap: 4,
                }}>
                <span style={{ fontSize: 10 }}>{icon}</span> {label}
              </button>
            ))}
          </div>
        </div>
      </header>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Sidebar */}
        <aside style={{
          width: 270, borderRight: "1px solid #151d2e", padding: 14,
          overflowY: "auto", flexShrink: 0, background: "#0a1020",
        }}>
          {/* Stats */}
          <div style={{ marginBottom: 18 }}>
            <div style={{
              fontSize: 9, fontWeight: 600, textTransform: "uppercase",
              letterSpacing: "0.12em", color: "#475569", marginBottom: 8,
              fontFamily: "'IBM Plex Mono', monospace",
            }}>Pregled baze</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
              {[
                { label: "Osobe", value: stats?.total_persons ?? "—", color: ENTITY_COLORS.Person },
                { label: "Firme", value: stats?.total_companies ?? "—", color: ENTITY_COLORS.Company },
                { label: "Ugovori", value: stats?.total_contracts ?? "—", color: ENTITY_COLORS.Contract },
                { label: "Institucije", value: stats?.total_institutions ?? "—", color: ENTITY_COLORS.Institution },
              ].map((s) => (
                <div key={s.label} style={{
                  background: "#111827", borderRadius: 8, padding: "8px 10px",
                  borderLeft: `3px solid ${s.color}`,
                  animation: loading ? "pulse 1.5s infinite" : "none",
                }}>
                  <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "'IBM Plex Mono', monospace", color: s.color }}>
                    {loading ? "…" : s.value}
                  </div>
                  <div style={{ fontSize: 9, color: "#64748b", marginTop: 2 }}>{s.label}</div>
                </div>
              ))}
            </div>

            {/* Risk summary card */}
            <div style={{
              background: "#111827", borderRadius: 8, padding: "10px 12px", marginTop: 6,
              borderLeft: `3px solid ${SEVERITY_COLORS[riskSummary?.risk_level || "low"]}`,
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div>
                  <div style={{
                    fontSize: 18, fontWeight: 700,
                    color: SEVERITY_COLORS[riskSummary?.risk_level || "low"],
                    fontFamily: "'IBM Plex Mono', monospace",
                  }}>
                    {alerts.length}
                  </div>
                  <div style={{ fontSize: 9, color: "#64748b" }}>Detektovano</div>
                </div>
                <div style={{ textAlign: "right" }}>
                  {riskSummary ? (
                    <RiskBadge level={riskSummary.risk_level} />
                  ) : (
                    <div style={{ fontSize: 13, fontWeight: 600, color: "#f59e0b", fontFamily: "'IBM Plex Mono', monospace" }}>
                      {formatRSD(totalContractValue)}
                    </div>
                  )}
                  <div style={{ fontSize: 9, color: "#64748b", marginTop: 3 }}>
                    {riskSummary ? "Rizik" : "RSD ukupno"}
                  </div>
                </div>
              </div>

              {riskSummary?.severity_counts && (
                <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                  {Object.entries(riskSummary.severity_counts).filter(([,v]) => v > 0).map(([sev, count]) => (
                    <div key={sev} style={{
                      fontSize: 9, fontFamily: "'IBM Plex Mono', monospace",
                      color: SEVERITY_COLORS[sev] || "#64748b",
                    }}>
                      {count}× {sev}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Legend */}
          <div style={{ marginBottom: 18 }}>
            <div style={{
              fontSize: 9, fontWeight: 600, textTransform: "uppercase",
              letterSpacing: "0.12em", color: "#475569", marginBottom: 8,
              fontFamily: "'IBM Plex Mono', monospace",
            }}>Legenda</div>
            {Object.entries(ENTITY_COLORS).slice(0, 7).map(([type, color]) => (
              <div key={type} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
                <div style={{ width: 8, height: 8, borderRadius: "50%", background: color, flexShrink: 0 }} />
                <span style={{ fontSize: 11, color: "#94a3b8" }}>
                  {{ Person: "Osoba", Company: "Firma", Institution: "Institucija", Contract: "Ugovor", PoliticalParty: "Stranka", Address: "Adresa", BudgetItem: "Budžet" }[type] || type}
                </span>
              </div>
            ))}
          </div>

          {/* Selected entity detail */}
          {selectedNode && (
            <div style={{
              background: "#111827", borderRadius: 8, padding: 12,
              border: `1px solid ${ENTITY_COLORS[selectedNode.type] || "#1e293b"}`,
              animation: "fadeIn 0.2s ease-out",
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{
                  fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em",
                  color: ENTITY_COLORS[selectedNode.type], fontWeight: 600,
                  fontFamily: "'IBM Plex Mono', monospace",
                }}>
                  {{ Person: "Osoba", Company: "Firma", Institution: "Institucija", Contract: "Ugovor", PoliticalParty: "Stranka" }[selectedNode.type] || selectedNode.type}
                </div>
                <button onClick={clearSelection} style={{
                  background: "none", border: "none", color: "#475569",
                  cursor: "pointer", fontSize: 16, lineHeight: 1, padding: 0,
                }}>×</button>
              </div>
              <div style={{ fontSize: 13, fontWeight: 600, marginTop: 5 }}>{selectedNode.name}</div>
              <div style={{ fontSize: 10, color: "#64748b", marginTop: 3, fontFamily: "'IBM Plex Mono', monospace" }}>
                ID: {selectedNode.id}
              </div>
              {selectedNode.props?.current_role && (
                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 2 }}>
                  Pozicija: {selectedNode.props.current_role}
                </div>
              )}
              {selectedNode.props?.value_rsd && (
                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 2 }}>
                  Vrednost: {formatRSD(selectedNode.props.value_rsd)} RSD
                </div>
              )}
              {selectedNode.props?.status && (
                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 2 }}>
                  Status: {selectedNode.props.status}
                </div>
              )}
              {!isDemo && (
                <button
                  onClick={() => exploreEntity(selectedNode.id, selectedNode.type)}
                  style={{
                    marginTop: 8, background: ENTITY_COLORS[selectedNode.type] + "22",
                    border: `1px solid ${ENTITY_COLORS[selectedNode.type]}44`,
                    color: "#e2e8f0", padding: "4px 10px", borderRadius: 4,
                    fontSize: 10, cursor: "pointer", fontFamily: "'IBM Plex Mono', monospace",
                  }}>
                  Istraži mrežu →
                </button>
              )}
            </div>
          )}

          {/* Selected alert detail */}
          {selectedAlert && !selectedNode && (
            <div style={{
              background: "#111827", borderRadius: 8, padding: 12,
              border: `1px solid ${SEVERITY_COLORS[selectedAlert.severity]}44`,
              animation: "fadeIn 0.2s ease-out",
            }}>
              <div style={{
                fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em",
                color: SEVERITY_COLORS[selectedAlert.severity], fontWeight: 600,
                fontFamily: "'IBM Plex Mono', monospace",
                display: "flex", justifyContent: "space-between", alignItems: "center",
              }}>
                <span>{PATTERN_LABELS[selectedAlert.pattern_type] || selectedAlert.pattern_type}</span>
                <RiskBadge level={selectedAlert.severity} />
              </div>
              <div style={{ fontSize: 11, marginTop: 8, lineHeight: 1.6, color: "#cbd5e1" }}>
                {selectedAlert.description}
              </div>
              <button onClick={clearSelection} style={{
                marginTop: 8, background: "#1e293b", border: "none",
                color: "#e2e8f0", padding: "4px 10px", borderRadius: 4,
                fontSize: 10, cursor: "pointer",
              }}>Poništi selekciju</button>
            </div>
          )}
        </aside>

        {/* Main content */}
        <main style={{ flex: 1, position: "relative", overflow: "hidden" }}>
          {activeTab === "graph" && (
            <ForceGraph
              nodes={nodes}
              edges={edges}
              onNodeClick={handleNodeClick}
              highlightIds={highlightIds}
            />
          )}

          {activeTab === "alerts" && (
            <div style={{ padding: 20, overflowY: "auto", height: "100%" }}>
              <div style={{
                fontSize: 9, fontWeight: 600, textTransform: "uppercase",
                letterSpacing: "0.12em", color: "#475569", marginBottom: 14,
                fontFamily: "'IBM Plex Mono', monospace",
              }}>
                Detektovani obrasci ({filteredAlerts.length})
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {filteredAlerts.map((alert, i) => (
                  <div key={i}
                    onClick={() => handleAlertClick(alert)}
                    style={{
                      background: "#111827", borderRadius: 8, padding: 14,
                      borderLeft: `4px solid ${SEVERITY_COLORS[alert.severity]}`,
                      cursor: "pointer", transition: "all 0.15s",
                      animation: `fadeIn 0.3s ease-out ${i * 0.05}s both`,
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.background = "#1a2332"}
                    onMouseLeave={(e) => e.currentTarget.style.background = "#111827"}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                      <span style={{
                        fontSize: 10, fontWeight: 600, textTransform: "uppercase",
                        color: SEVERITY_COLORS[alert.severity],
                        fontFamily: "'IBM Plex Mono', monospace",
                      }}>{PATTERN_LABELS[alert.pattern_type] || alert.pattern_type}</span>
                      <RiskBadge level={alert.severity} />
                    </div>
                    <div style={{ fontSize: 12, lineHeight: 1.6, color: "#cbd5e1" }}>
                      {alert.description}
                    </div>
                    {(alert.contract_value || alert.value_rsd || alert.total_value) && (
                      <div style={{
                        fontSize: 10, color: "#f59e0b", marginTop: 6,
                        fontFamily: "'IBM Plex Mono', monospace", fontWeight: 600,
                      }}>
                        {formatRSD(alert.contract_value || alert.value_rsd || alert.total_value)} RSD
                      </div>
                    )}
                    <div style={{ fontSize: 9, color: "#475569", marginTop: 6, fontFamily: "'IBM Plex Mono', monospace", display: "flex", gap: 8 }}>
                      <span style={{ color: "#3b82f688" }}>◉ Klikni za detalje i objašnjenje</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {activeTab === "data" && (
            <div style={{ padding: 20, overflowY: "auto", height: "100%" }}>
              <div style={{
                fontSize: 9, fontWeight: 600, textTransform: "uppercase",
                letterSpacing: "0.12em", color: "#475569", marginBottom: 14,
                fontFamily: "'IBM Plex Mono', monospace",
              }}>Izvori podataka</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {[
                  { name: "APR — Registar privrednih subjekata", status: "Spreman", url: "pretraga.apr.gov.rs", count: `${stats?.total_companies || "?"} firmi` },
                  { name: "Portal javnih nabavki", status: "Spreman", url: "jnportal.ujn.gov.rs", count: `${stats?.total_contracts || "?"} ugovora` },
                  { name: "RIK — Izborna komisija", status: "Planiran", url: "rik.parlament.gov.rs", count: "—" },
                  { name: "data.gov.rs — Otvoreni podaci", status: "Planiran", url: "data.gov.rs", count: "—" },
                  { name: "Službeni glasnik", status: "Planiran", url: "pravno-informacioni-sistem.rs", count: "—" },
                  { name: "RGZ — Katastar", status: "Planiran", url: "rgz.gov.rs", count: "—" },
                ].map((src, i) => (
                  <div key={i} style={{
                    background: "#111827", borderRadius: 8, padding: 14,
                    border: "1px solid #1e293b",
                    animation: `fadeIn 0.3s ease-out ${i * 0.08}s both`,
                  }}>
                    <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>{src.name}</div>
                    <div style={{ fontSize: 10, color: "#475569", fontFamily: "'IBM Plex Mono', monospace", marginBottom: 4 }}>{src.url}</div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 6 }}>
                      <span style={{
                        fontSize: 9, padding: "2px 8px", borderRadius: 4,
                        background: src.status === "Spreman" ? "#10b98122" : "#47556922",
                        color: src.status === "Spreman" ? "#10b981" : "#475569",
                        fontFamily: "'IBM Plex Mono', monospace", fontWeight: 600,
                      }}>{src.status}</span>
                      <span style={{ fontSize: 10, color: "#94a3b8" }}>{src.count}</span>
                    </div>
                  </div>
                ))}
              </div>

              <div style={{
                fontSize: 9, fontWeight: 600, textTransform: "uppercase",
                letterSpacing: "0.12em", color: "#475569", marginTop: 24, marginBottom: 14,
                fontFamily: "'IBM Plex Mono', monospace",
              }}>Detekcioni obrasci</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {[
                  { fn: "conflict_of_interest()", desc: "Sukob interesa (funkcioner ↔ porodica ↔ firma ↔ ugovor)" },
                  { fn: "ghost_employees()", desc: "Fantomski zaposleni (duplikati u evidencijama)" },
                  { fn: "shell_company_clusters()", desc: "Shell kompanije (ista adresa, isti direktori)" },
                  { fn: "single_bidder_contracts()", desc: "Ugovori sa jednim ponuđačem" },
                  { fn: "revolving_door()", desc: "Rotirajuća vrata (regulator → regulisani)" },
                  { fn: "budget_self_allocation()", desc: "Samododeljivanje budžetskih amandmana" },
                  { fn: "contract_splitting()", desc: "Deljenje ugovora (ispod praga, vremenski blizu)" },
                  { fn: "political_donor_contracts()", desc: "Donator stranke → dobijeni ugovori" },
                ].map((p, i) => (
                  <div key={i} style={{
                    background: "#111827", borderRadius: 6, padding: "8px 12px",
                    fontSize: 11, fontFamily: "'IBM Plex Mono', monospace",
                    color: "#94a3b8", borderLeft: "2px solid #3b82f6",
                    animation: `fadeIn 0.3s ease-out ${i * 0.06}s both`,
                  }}>
                    <span style={{ color: "#3b82f6" }}>{p.fn}</span> — {p.desc}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Floating info bar */}
          {activeTab === "graph" && (
            <div style={{
              position: "absolute", bottom: 14, left: 14,
              background: "#0a1020ee", borderRadius: 8, padding: "6px 12px",
              border: "1px solid #151d2e", fontSize: 9,
              fontFamily: "'IBM Plex Mono', monospace", color: "#475569",
              display: "flex", gap: 12, alignItems: "center",
            }}>
              <span>{nodes.length} čvorova</span>
              <span>·</span>
              <span>{edges.length} veza</span>
              <span>·</span>
              <span>Klikni čvor za detalje</span>
              {isDemo && (
                <>
                  <span>·</span>
                  <span style={{ color: "#f59e0b" }}>Demo podaci</span>
                </>
              )}
            </div>
          )}
        </main>
      </div>
    </div>
    </>
  );
}
