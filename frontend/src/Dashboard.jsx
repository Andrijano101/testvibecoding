import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import * as d3 from "d3";

const API_BASE = window.__API_BASE__ || "http://localhost:8000";

const ENTITY_COLORS = {
  Person: "#f59e0b",
  Company: "#3b82f6",
  Institution: "#10b981",
  Contract: "#ef4444",
  PoliticalParty: "#8b5cf6",
  Address: "#6b7280",
  BudgetItem: "#ec4899",
  Property: "#06b6d4",
};

const SEVERITY_COLORS = {
  critical: "#dc2626",
  high: "#f97316",
  medium: "#eab308",
  low: "#6b7280",
};

const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 };

const DEMO_NODES = [
  { id: "P-001", name: "Petar Petrović", type: "Person", props: { current_role: "Direktor sektora" } },
  { id: "P-002", name: "Marija Petrović", type: "Person", props: { current_role: "Osnivač" } },
  { id: "P-003", name: "Nikola Jovanović", type: "Person", props: { current_role: "Član odbora" } },
  { id: "C-001", name: "TechSerb DOO", type: "Company", props: { status: "Aktivan" } },
  { id: "C-002", name: "DataLink DOO", type: "Company", props: { status: "Aktivan" } },
  { id: "I-001", name: "Ministarstvo finansija", type: "Institution", props: {} },
  { id: "CT-001", name: "IT infrastruktura - faza 1", type: "Contract", props: { value_rsd: 45000000 } },
  { id: "CT-002", name: "Softverska podrška", type: "Contract", props: { value_rsd: 12000000 } },
  { id: "PP-001", name: "Stranka napretka", type: "PoliticalParty", props: {} },
];

const DEMO_EDGES = [
  { source: "P-001", target: "I-001", relationship: "EMPLOYED_BY" },
  { source: "P-001", target: "P-002", relationship: "FAMILY_OF" },
  { source: "P-002", target: "C-001", relationship: "OWNS" },
  { source: "P-003", target: "C-002", relationship: "DIRECTS" },
  { source: "I-001", target: "CT-001", relationship: "AWARDED_CONTRACT" },
  { source: "I-001", target: "CT-002", relationship: "AWARDED_CONTRACT" },
  { source: "C-001", target: "CT-001", relationship: "WON_CONTRACT" },
  { source: "P-001", target: "PP-001", relationship: "MEMBER_OF" },
];

const DEMO_ALERTS = [
  {
    pattern_type: "conflict_of_interest", severity: "critical",
    official_name: "Petar Petrović", official_role: "Direktor sektora",
    institution: "Ministarstvo finansija", family_member: "Marija Petrović",
    company_name: "TechSerb DOO", contract_title: "IT infrastruktura - faza 1",
    contract_value: 45000000, award_date: "2023-06-15",
  },
  {
    pattern_type: "single_bidder", severity: "high",
    contract_title: "IT infrastruktura - faza 1", value_rsd: 45000000,
    institution: "Ministarstvo finansija", winner: "TechSerb DOO", award_date: "2023-06-15",
  },
  {
    pattern_type: "contract_splitting", severity: "medium",
    institution: "Ministarstvo finansija", company_name: "TechSerb DOO",
    num_contracts: 2, total_value: 53500000, first_date: "2023-01-10", last_date: "2023-03-22",
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
  repeated_winner: "Stalni pobednik",
  new_company_big_contract: "Nova firma — veliki ugovor",
  institutional_monopoly: "Institucionalni monopol",
  samododeljivanje_proxy: "Poslanik/Funkcioner — direktor firme koja dobija ugovore",
  direct_official_contractor: "Funkcioner direktno na obe strane ugovora",
  ghost_director: "Fantomski direktor",
};

// Real Serbian government portal URLs for verification
const SOURCE_PORTALS = {
  apr:        { label: "APR — Registar privrednih subjekata", url: "https://pretraga.apr.gov.rs" },
  procurement:{ label: "Portal javnih nabavki", url: "https://jnportal.ujn.gov.rs/tender-documents/search" },
  officials:  { label: "Javni funkcioneri — data.gov.rs", url: "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/" },
  party_fin:  { label: "Finansiranje stranaka — ACAS", url: "https://www.acas.rs/finansiranje-politickih-subjekata/" },
  gazette:    { label: "Službeni glasnik", url: "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection" },
  parliament: { label: "Poslanici — Parlament RS", url: "https://www.parlament.gov.rs/members-of-parliament" },
  acas_prop:  { label: "Imovinski registar — ACAS", url: "https://www.acas.rs/imovinski-registar/" },
  budget:     { label: "Budžet RS — Ministarstvo finansija", url: "https://www.mfin.gov.rs/dokumenti/budzet/" },
  rgz:        { label: "Katastar nekretnina — RGZ", url: "https://rgz.gov.rs/usluge/eLine" },
};

// APR deep link by maticni broj
const aprLink = (mb) => mb ? `https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=${mb}` : SOURCE_PORTALS.apr.url;

const PATTERN_EXPLANATIONS = {
  conflict_of_interest: {
    icon: "⚖",
    title: "Sukob interesa",
    why: "Funkcioner koji direktno odlučuje o dodeli ugovora ima porodičnog člana koji je vlasnik ili direktor firme koja je dobila taj ugovor od iste institucije. Ovo je klasičan obrazac korupcije koji narušava princip nepristrasnosti u javnim nabavkama.",
    how: "(Funkcioner)-[EMPLOYED_BY]->(Institucija)\n(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n(Firma)-[WON_CONTRACT]->(Ugovor)\n(Porodični član)-[OWNS|DIRECTS]->(Firma)\n(Funkcioner)-[FAMILY_OF]-(Porodični član)\n\nSvi čvorovi moraju biti istovremeno prisutni.",
    sourcePortals: ["apr", "procurement", "officials"],
    sources: ["APR — vlasništvo i direktorska mesta u firmama", "Portal javnih nabavki — dodeljeni ugovori", "Evidencije funkcionera — radno mesto u instituciji"],
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
    icon: "👻",
    title: "Fantomski zaposleni",
    why: "Isto lice (isti normalizovani naziv) pojavljuje se u platnom spisku dve ili više različitih institucija sa različitim identifikatorima. Ukazuje na lažno zaposlenje ili isplatu plata za nepostojeće radnike.",
    how: "(P1:Person {name_normalized: X})-[EMPLOYED_BY]->(I1)\n(P2:Person {name_normalized: X})-[EMPLOYED_BY]->(I2)\ngde P1.person_id != P2.person_id i I1 != I2",
    sourcePortals: ["officials", "gazette", "parliament"],
    sources: ["Evidencije javnih funkcionera — data.gov.rs", "Službeni glasnik — rešenja o postavljenju", "RIK — kadrovske evidencije"],
    fields: [
      { key: "name_1", label: "Ime (evidencija 1)" },
      { key: "institution_1", label: "Institucija 1" },
      { key: "name_2", label: "Ime (evidencija 2)" },
      { key: "institution_2", label: "Institucija 2" },
      { key: "normalized_name", label: "Normalizovano ime" },
    ],
  },
  shell_company_cluster: {
    icon: "🐚",
    title: "Klaster shell kompanija",
    why: "Tri ili više firmi registrovanih na istoj adresi kolektivno osvajaju javne ugovore. Čest mehanizam za rasipanje ugovora između povezanih firmi radi zaobilaženja pragova nabavki.",
    how: "(Adresa)<-[REGISTERED_AT]-(C1)\n(Adresa)<-[REGISTERED_AT]-(C2)\n(Adresa)<-[REGISTERED_AT]-(C3)\n...\nGde svaka kompanija ima WON_CONTRACT odnos.\nSuma vrednosti svih ugovora = ukupna izloženost.",
    sourcePortals: ["apr", "procurement"],
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
    icon: "1️⃣",
    title: "Ugovor sa jednim ponuđačem",
    why: "Javna nabavka primila je samo jednu ponudu, što drastično smanjuje konkurenciju. Posebno sumnjivo kada se ponavlja sa istom firmom ili institucijom, ili kada je vrednost visoka.",
    how: "(Institucija)-[AWARDED_CONTRACT]->(Ugovor {num_bidders: 1})\n(Firma)-[WON_CONTRACT]->(Ugovor)\ngde Ugovor.value_rsd >= prag (podrazumevano 1.000.000 RSD)",
    sourcePortals: ["procurement"],
    sources: ["Portal javnih nabavki — broj ponuda i pobednici nabavke"],
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
    icon: "🔄",
    title: "Rotirajuća vrata",
    why: "Bivši državni funkcioner napustio je instituciju i preuzeo rukovodeću poziciju u privatnoj firmi koja potom dobija ugovore od te iste institucije. Lice koristi insajderska znanja i poslovne kontakte.",
    how: "(Osoba)-[EMPLOYED_BY {until: datum}]->(Institucija)\n(Osoba)-[DIRECTS|OWNS {since: datum >= until}]->(Firma)\nOPCIONALNO:\n(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n(Firma)-[WON_CONTRACT]->(Ugovor)",
    sourcePortals: ["gazette", "apr", "procurement"],
    sources: ["Službeni glasnik — rešenja o razrešenju", "APR — direktorska imenovanja", "Portal javnih nabavki — ugovori"],
    fields: [
      { key: "person_name", label: "Osoba" },
      { key: "former_institution", label: "Bivša institucija" },
      { key: "govt_role", label: "Bivša pozicija" },
      { key: "left_govt", label: "Datum odlaska" },
      { key: "company_name", label: "Nova firma" },
      { key: "company_role", label: "Nova pozicija" },
      { key: "joined_company", label: "Datum ulaska" },
      { key: "contracts_between", label: "Ugovora između" },
      { key: "total_contract_value", label: "Ukupna vrednost" },
    ],
  },
  budget_self_allocation: {
    icon: "💰",
    title: "Samododeljivanje budžeta",
    why: "Funkcioner je odobrio budžetsku stavku, a ugovor finansiran iz te stavke dobila je firma sa kojom ima porodičnu ili vlasničku vezu. Direktni sukob interesa na nivou budžetskog procesa.",
    how: "(Osoba)-[ALLOCATED_BY]-(BudžetStavka)\n(BudžetStavka)-[FUNDS]->(Ugovor)\n(Firma)-[WON_CONTRACT]->(Ugovor)\nGde postoji put dužine 1-3 između Osobe i Firme\nkroz FAMILY_OF, OWNS ili DIRECTS odnose.",
    sourcePortals: ["budget", "procurement", "apr"],
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
    icon: "✂",
    title: "Deljenje ugovora",
    why: "Ista firma dobija više ugovora od iste institucije u kratkom vremenskom periodu, pri čemu su svi ispod zakonskog praga za obaveznu javnu licitaciju. Zbir vrednosti prelazi prag — klasičan način zaobilaženja procedura.",
    how: "(Institucija)-[AWARDED_CONTRACT]->(CT1, CT2...)\n(Firma)-[WON_CONTRACT]->(CT1, CT2...)\ngde: CT.value_rsd < prag (npr. 6M)\n  i: CT.value_rsd > prag * 0.5\n  i: count >= 2\n  i: svi ugovori u roku od 90 dana",
    sourcePortals: ["procurement"],
    sources: ["Portal javnih nabavki — hronologija ugovora po naručiocu i dobavljaču"],
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
    icon: "🤝",
    title: "Donator stranke → Ugovor",
    why: "Firma koja je finansirala političku stranku osvaja javne ugovore od institucija kojima rukovode članovi te stranke. Obrazac poznat kao 'pay-to-play' — donacija kao investicija u buduće ugovore.",
    how: "(Firma)-[DONATED_TO]->(PolitičkaStranka)\n(Firma)-[WON_CONTRACT]->(Ugovor)\n(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\nOPCIONALNO:\n(Osoba)-[MEMBER_OF]->(PolitičkaStranka)\n(Osoba)-[EMPLOYED_BY]->(Institucija)",
    sourcePortals: ["party_fin", "apr", "procurement"],
    sources: ["ACAS — finansiranje stranaka", "APR — donatori", "Portal javnih nabavki — ugovori"],
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
  repeated_winner: {
    icon: "🏆",
    title: "Stalni pobednik",
    why: "Ista firma pobeđuje na javnim nabavkama kod iste institucije više puta zaredom, osvajajući dominantan deo njenog ukupnog budžeta za nabavke. U zdravom sistemu, različite firme trebalo bi da pobede u različitim raspisima — stalno isti pobednik ukazuje da:\n\n• Konkurs je pisan po meri te firme (technički zahtevi, rokovi, specifikacije koji odgovaraju samo jednom ponuđaču)\n• Evaluaciona komisija sistematski favorizuje isti ponuđač\n• Ostale firme su obeshrabrene ili onemogućene da apliciraju (pritisak, neformalni dogovori)\n• Može biti posledica korupcije, personalnih veza između direktora firme i funkcionera, ili prethodnih 'revolving door' prelaza\n\nZakon o javnim nabavkama Srbije propisuje princip konkurentnosti — ponavljano osvajanje od jednog ponuđača je signal za reviziju.",
    how: "(Firma)-[WON_CONTRACT]->(CT1, CT2, CT3...)\n(Institucija)-[AWARDED_CONTRACT]->(CT1, CT2, CT3...)\ngde: count(ugovori firma/institucija) >= 3\n  i: firma.ugovori / institucija.ukupno_ugovora >= 50%\n\nDodatni signal:\n- sve pobede u kratkom periodu (< 24 meseca)\n- kombinacija sa single_bidder na istim raspisima\n- visoka prosečna vrednost ugovora",
    sourcePortals: ["procurement"],
    sources: ["Portal javnih nabavki — istorija dodele ugovora po naručiocu i dobavljaču"],
    fields: [
      { key: "company_name", label: "Stalni pobednik" },
      { key: "institution", label: "Institucija" },
      { key: "win_count", label: "Broj pobeda" },
      { key: "total_value", label: "Ukupna vrednost" },
      { key: "first_win", label: "Prva pobeda" },
      { key: "last_win", label: "Poslednja pobeda" },
      { key: "share_pct", label: "% budžeta institucije" },
    ],
  },
  new_company_big_contract: {
    icon: "🆕",
    title: "Nova firma — veliki ugovor",
    why: "Novoosnovana firma (mlađa od 3 godine) osvaja javne ugovore visoke vrednosti bez dokazanog iskustva i poslovne istorije. Zakon o javnim nabavkama zahteva od ponuđača dokaze o referentnim ugovorima i finansijskom kapacitetu — pa se nameće pitanje kako firma bez istorije ispunjava te uslove.\n\nKarakterstični scenariji:\n\n• Firma osnovana neposredno pre raspisivanja konkursa — kao da je 'napravljena' posebno za taj tender\n• Ishodišna firma: postojeći direktor osniva novu firmu i 'prebacuje' ugovore na nju\n• Politički podobna firma: osnivač ili direktor ima veze sa strankom koja kontroliše instituciju\n• 'Školjka': nova firma nema zaposlenih ni kapaciteta — posao obavljaju kooperanti\n\nBG BUS PREVOZ d.o.o. je primer: osnovan juna 2024, u roku od meseci dobio je ugovor vrednosti 142 milijarde RSD (gradski prevoz Beograda). Bez prethodnih referenci, bez poslovne istorije.",
    how: "(Firma)-[:WON_CONTRACT]->(Ugovor)\ngde: Firma.founding_date IS NOT NULL\n  i: award_year - founding_year <= 3\n  i: contract_value >= 5.000.000 RSD\n\nSeverity:\n  age = 0 (ista godina osnivanja): CRITICAL\n  age = 1 + value >= 10M: CRITICAL\n  age <= 2: HIGH\n  age = 3: MEDIUM",
    sourcePortals: ["apr", "procurement"],
    sources: ["APR — datum osnivanja firme", "Portal javnih nabavki — datum i vrednost ugovora"],
    fields: [
      { key: "company_name", label: "Firma" },
      { key: "founded", label: "Datum osnivanja" },
      { key: "age_at_award", label: "Starost firme (god.)" },
      { key: "num_contracts", label: "Broj ugovora" },
      { key: "total_value", label: "Ukupna vrednost" },
      { key: "contract_title", label: "Najveći ugovor" },
      { key: "contract_value", label: "Vrednost najvećeg" },
      { key: "award_date", label: "Datum dodele" },
      { key: "institution", label: "Naručilac" },
    ],
  },
  samododeljivanje_proxy: {
    icon: "🏛",
    title: "Poslanik/Funkcioner — direktor firme koja dobija ugovore",
    why: "Narodni poslanik ili javni funkcioner istovremeno obavlja direktorsku funkciju u firmi koja osvaja javne ugovore. Ovo je direktni sukob interesa — zakon o javnim nabavkama zahteva nepristrasnost, ali lice koje kontroliše firmu može koristiti politički uticaj da osigura ugovore. Primer: Dušan Bajatović (poslanik SPS) generalni je direktor JP Srbijagasa, koji dobija ugovore vredne milijarde RSD.",
    how: "(Poslanik/Funkcioner)-[EMPLOYED_BY]->(Skupština/Vlada)\n(Poslanik/Funkcioner)-[DIRECTS]->(Firma)\n(Firma)-[WON_CONTRACT]->(Ugovor)\n(BilokojInstitucija)-[AWARDED_CONTRACT]->(Ugovor)\n\nNe zahteva da institucija koja zapošljava bude ista koja dodeljuje ugovor —\npolitički uticaj deluje posredno.",
    sourcePortals: ["parliament", "apr", "procurement"],
    sources: ["Otvoreni Parlament — imovinska karta poslanika (direktorska mesta)", "APR — registar preduzetnika i firmi", "Portal javnih nabavki — ugovori"],
    fields: [
      { key: "official_name", label: "Poslanik/Funkcioner" },
      { key: "official_role", label: "Javna pozicija" },
      { key: "employer_institution", label: "Institucija" },
      { key: "company_name", label: "Firma kojom rukovodi" },
      { key: "contract_title", label: "Ugovor" },
      { key: "contract_value", label: "Vrednost ugovora" },
      { key: "awarding_institution", label: "Naručilac" },
      { key: "award_date", label: "Datum dodele" },
    ],
  },
  direct_official_contractor: {
    icon: "⚡",
    title: "Funkcioner direktno na obe strane ugovora",
    why: "Isti funkcioner je zaposlen u instituciji koja dodeljuje ugovor I istovremeno rukovodi firmom koja taj ugovor dobija — bez posrednika (porodičnog člana i sl.). Najdirektniji oblik sukoba interesa: ista osoba kontroliše i naručioca i dobavljača.",
    how: "(Funkcioner)-[EMPLOYED_BY]->(Institucija)\n(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n(Firma)-[WON_CONTRACT]->(Ugovor)\n(Funkcioner)-[DIRECTS|OWNS]->(Firma)\n\nSvi elementi moraju biti isti — Funkcioner je i kod naručioca i u firmi pobedniku.",
    sourcePortals: ["officials", "apr", "procurement"],
    sources: ["Evidencije funkcionera", "APR — direktorska mesta", "Portal javnih nabavki"],
    fields: [
      { key: "official_name", label: "Funkcioner" },
      { key: "official_role", label: "Pozicija" },
      { key: "institution", label: "Institucija naručilac" },
      { key: "company_name", label: "Firma" },
      { key: "contract_title", label: "Ugovor" },
      { key: "contract_value", label: "Vrednost" },
      { key: "award_date", label: "Datum" },
    ],
  },
  ghost_director: {
    icon: "👤",
    title: "Fantomski direktor",
    why: "Lice je formalno direktno više firmi koje zajedno osvajaju ugovore od iste institucije — ali nema fizičku mogućnost da stvarno rukovodi svima. Čest obrazac pri korišćenju 'front' kompanija: nominalni direktor potpisuje dokumenta, a stvarni vlasnik ostaje u senci.",
    how: "(Osoba)-[DIRECTS]->(Firma1)\n(Osoba)-[DIRECTS]->(Firma2)\n(Firma1)-[WON_CONTRACT]->(Ugovor1)\n(Firma2)-[WON_CONTRACT]->(Ugovor2)\n(IstaInstitucija)-[AWARDED_CONTRACT]->(Ugovor1)\n(IstaInstitucija)-[AWARDED_CONTRACT]->(Ugovor2)\n\nNajmanje 2 firme, iste institucije.",
    sourcePortals: ["apr", "procurement"],
    sources: ["APR — direktorska imenovanja", "Portal javnih nabavki — pobednici nabavki"],
    fields: [
      { key: "director_name", label: "Direktor" },
      { key: "institution", label: "Institucija" },
      { key: "num_companies", label: "Broj firmi" },
      { key: "total_value", label: "Ukupna vrednost" },
    ],
  },
  institutional_monopoly: {
    icon: "🏛",
    title: "Institucionalni monopol",
    why: "Jedna firma prima 70% ili više celokupnog budžeta javnih nabavki jedne institucije. Ovo nije slučajnost — ukazuje na sistemsko zarobljavanje nabavnog procesa ('procurement capture'):\n\n• Konkursna dokumentacija je konstruisana tako da praktično samo jedna firma može ispuniti uslove\n• Evaluatori imaju uputstvo ili implicitni pritisak da biraju unapred određenog pobednika\n• Institucija je u neformalno zavisnom odnosu sa firmom (personalne veze, podmićivanje, politički uticaj)\n• Ostale firme su naučile da ne apliciraju jer znaju da nemaju šanse\n\nPrincip: ako je ceo javni budžet jedne institucije faktički privatizovan od strane jedne firme, to nije tržišna utakmica — to je monopol na javnom novcu. Svaki evro koji ide jednoj firmi je evro koji nije podložan stvarnoj konkurenciji.\n\nPrema EU standardima i SIGMA metodologiji, koncentracija > 50% kod jednog dobavljača za jednu instituciju se klasifikuje kao crvena zastavica za korupciju.",
    how: "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n(Firma)-[WON_CONTRACT]->(Ugovor)\n\nAGREGACIJA:\n  firma_vrednost = SUM(value_rsd gde pobednik = Firma)\n  institucija_ukupno = SUM(value_rsd sve nabavke)\n  udeo = firma_vrednost / institucija_ukupno\n\nUSLOV:\n  udeo >= 0.70  (tj. firma dobija >= 70% budžeta)\n  firma_vrednost >= 10.000.000 RSD  (minimalni prag relevantnosti)",
    sourcePortals: ["procurement"],
    sources: ["Portal javnih nabavki (UJN) — svi ugovori institucije grupisani po dobavljaču"],
    fields: [
      { key: "institution", label: "Institucija" },
      { key: "company_name", label: "Dominantni dobavljač" },
      { key: "company_pct_of_institution", label: "Udeo u budžetu (%)" },
      { key: "company_total_value", label: "Vrednost ugovora firme" },
      { key: "institution_total_value", label: "Ukupan budžet institucije" },
      { key: "num_contracts", label: "Broj ugovora" },
    ],
  },
};

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
  if (value == null || value === "") return "—";
  const n = Number(value);
  if (isNaN(n)) return String(value);
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B RSD`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M RSD`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)}K RSD`;
  return n.toLocaleString("sr-RS") + " RSD";
}

function Spinner({ size = 20 }) {
  return (
    <div style={{
      width: size, height: size, border: "2px solid #334155",
      borderTop: "2px solid #3b82f6", borderRadius: "50%",
      animation: "spin 0.8s linear infinite",
    }} />
  );
}

function RiskBadge({ level }) {
  const colors = {
    critical: { bg: "#dc262622", border: "#dc2626", text: "#fca5a5" },
    high:     { bg: "#f9731622", border: "#f97316", text: "#fdba74" },
    medium:   { bg: "#eab30822", border: "#eab308", text: "#fde047" },
    low:      { bg: "#6b728022", border: "#6b7280", text: "#9ca3af" },
  };
  const c = colors[level] || colors.low;
  return (
    <span style={{
      fontSize: 9, padding: "2px 8px", borderRadius: 4,
      background: c.bg, border: `1px solid ${c.border}`,
      color: c.text, fontWeight: 700, textTransform: "uppercase",
      fontFamily: "'IBM Plex Mono', monospace", letterSpacing: "0.05em",
      whiteSpace: "nowrap",
    }}>{level}</span>
  );
}

// ── Pattern Detail Modal ────────────────────────────────────────
function PatternDetailModal({ alert, onClose, onShowOnGraph, onOpenEntity }) {
  const exp = PATTERN_EXPLANATIONS[alert.pattern_type] || {};
  const color = SEVERITY_COLORS[alert.severity] || "#6b7280";
  const [rawOpen, setRawOpen] = useState(false);

  const renderFieldValue = (key) => {
    const val = alert[key];
    if (val == null || val === "") return null;
    const isMonetary = key.includes("value") || key.includes("amount") || key.includes("total_value");
    if (isMonetary) return formatRSD(val);
    return String(val);
  };

  return (
    <div
      onClick={(e) => e.target === e.currentTarget && onClose()}
      style={{
        position: "fixed", inset: 0, zIndex: 1000,
        display: "flex", justifyContent: "flex-end",
        background: "rgba(0,0,0,0.65)", backdropFilter: "blur(3px)",
      }}
    >
      <div style={{
        width: "min(580px, 96vw)", background: "#0d1525",
        borderLeft: "1px solid #1e293b", overflowY: "auto",
        display: "flex", flexDirection: "column",
        animation: "slideInRight 0.2s ease-out",
      }}>
        {/* Sticky header */}
        <div style={{
          padding: "18px 22px 14px", borderBottom: "1px solid #1e293b",
          position: "sticky", top: 0, zIndex: 2,
          background: "#0d1525ee", backdropFilter: "blur(8px)",
          borderTop: `3px solid ${color}`,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
              <span style={{ fontSize: 24, flexShrink: 0 }}>{exp.icon || "⚠"}</span>
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: 17, fontWeight: 700, color: "#f8fafc", letterSpacing: "-0.02em" }}>
                  {exp.title || PATTERN_LABELS[alert.pattern_type] || alert.pattern_type}
                </div>
                <div style={{ marginTop: 5 }}>
                  <RiskBadge level={alert.severity} />
                </div>
              </div>
            </div>
            <button onClick={onClose} style={{
              background: "#1e293b", border: "1px solid #334155", color: "#94a3b8",
              width: 30, height: 30, borderRadius: 6, cursor: "pointer",
              fontSize: 18, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>×</button>
          </div>
        </div>

        <div style={{ padding: "22px", display: "flex", flexDirection: "column", gap: 22 }}>

          {/* Evidence */}
          <section>
            <SectionTitle icon="🔍" title="Detektovani entiteti" />
            <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
              {(exp.fields || []).map(({ key, label }) => {
                const val = renderFieldValue(key);
                if (!val) return null;
                const isMonetary = key.includes("value") || key.includes("amount") || key.includes("total_value");
                return (
                  <div key={key} style={{
                    display: "flex", gap: 10, padding: "7px 12px",
                    background: "#111827", borderRadius: 6, alignItems: "flex-start",
                  }}>
                    <span style={{
                      fontSize: 10, color: "#475569", minWidth: 160, flexShrink: 0,
                      fontFamily: "'IBM Plex Mono', monospace", paddingTop: 1,
                    }}>{label}</span>
                    <span style={{
                      fontSize: 12, fontWeight: 500, wordBreak: "break-word",
                      color: isMonetary ? "#f59e0b" : "#e2e8f0",
                      fontFamily: isMonetary ? "'IBM Plex Mono', monospace" : "inherit",
                      fontWeight: isMonetary ? 700 : 500,
                    }}>{val}</span>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Why suspicious */}
          {exp.why && (
            <section>
              <SectionTitle icon="⚠" title="Zašto je sumnjivo" />
              <p style={{
                fontSize: 13, lineHeight: 1.75, color: "#cbd5e1", margin: 0,
                padding: "12px 14px", background: "#111827", borderRadius: 6,
                borderLeft: `3px solid ${color}`,
              }}>{exp.why}</p>
            </section>
          )}

          {/* How detected */}
          {exp.how && (
            <section>
              <SectionTitle icon="◎" title="Kako je detektovano (put u grafu)" />
              <pre style={{
                fontSize: 11, lineHeight: 1.8, color: "#7dd3fc",
                fontFamily: "'IBM Plex Mono', monospace", margin: 0,
                whiteSpace: "pre-wrap", wordBreak: "break-word",
                background: "#060d1a", padding: "14px 16px",
                borderRadius: 6, border: "1px solid #1e3a5f",
              }}>{exp.how}</pre>
            </section>
          )}

          {/* Sources */}
          {(exp.sources?.length > 0 || exp.sourcePortals?.length > 0) && (
            <section>
              <SectionTitle icon="◈" title="Korišćeni izvori podataka" />
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {(exp.sources || []).map((src, i) => (
                  <div key={i} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 12px", background: "#111827", borderRadius: 6,
                  }}>
                    <div style={{ width: 7, height: 7, borderRadius: "50%", background: "#3b82f6", flexShrink: 0 }} />
                    <span style={{ fontSize: 12, color: "#94a3b8" }}>{src}</span>
                  </div>
                ))}
                {(exp.sourcePortals || []).map((key) => {
                  const portal = SOURCE_PORTALS[key];
                  if (!portal) return null;
                  return (
                    <a key={key} href={portal.url} target="_blank" rel="noopener noreferrer" style={{
                      display: "flex", alignItems: "center", gap: 10,
                      padding: "8px 12px", background: "#0d1f3c", borderRadius: 6,
                      border: "1px solid #1e3a5f", textDecoration: "none",
                      transition: "background 0.15s",
                    }}
                      onMouseEnter={e => e.currentTarget.style.background = "#132545"}
                      onMouseLeave={e => e.currentTarget.style.background = "#0d1f3c"}
                    >
                      <span style={{ fontSize: 11, color: "#7dd3fc", flexShrink: 0 }}>↗</span>
                      <span style={{ fontSize: 12, color: "#60a5fa" }}>{portal.label}</span>
                    </a>
                  );
                })}
                {alert.verification_url && (
                  <a href={alert.verification_url} target="_blank" rel="noopener noreferrer" style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 12px", background: "#1a1208", borderRadius: 6,
                    border: "1px solid #f59e0b44", textDecoration: "none",
                    transition: "background 0.15s",
                  }}
                    onMouseEnter={e => e.currentTarget.style.background = "#231908"}
                    onMouseLeave={e => e.currentTarget.style.background = "#1a1208"}
                  >
                    <span style={{ fontSize: 11, color: "#f59e0b", flexShrink: 0 }}>⊕</span>
                    <span style={{ fontSize: 12, color: "#fbbf24" }}>Proveri izvor direktno</span>
                  </a>
                )}
              </div>
            </section>
          )}

          {/* Raw JSON toggle */}
          <section>
            <button onClick={() => setRawOpen(o => !o)} style={{
              background: "none", border: "none", color: "#475569", cursor: "pointer",
              fontSize: 10, fontFamily: "'IBM Plex Mono', monospace", padding: 0,
              display: "flex", alignItems: "center", gap: 5,
            }}>
              <span style={{
                display: "inline-block",
                transform: rawOpen ? "rotate(90deg)" : "none",
                transition: "transform 0.15s",
              }}>▶</span>
              Sirovi podaci (JSON)
            </button>
            {rawOpen && (
              <pre style={{
                marginTop: 8, fontSize: 10, color: "#64748b",
                fontFamily: "'IBM Plex Mono', monospace",
                background: "#060c18", padding: "12px 14px",
                borderRadius: 6, border: "1px solid #1e293b",
                overflowX: "auto", whiteSpace: "pre-wrap", wordBreak: "break-all",
                maxHeight: 300, overflowY: "auto",
              }}>
                {JSON.stringify(alert, null, 2)}
              </pre>
            )}
          </section>

          {/* Entity quick-links */}
          {onOpenEntity && (alert.company_mb || alert.company_name) && (
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              {alert.company_mb && (
                <button onClick={() => onOpenEntity(alert.company_mb, "Company", alert.company_name || alert.company_mb)} style={{
                  flex: 1, padding: "9px 14px", borderRadius: 8, cursor: "pointer",
                  background: "#3b82f622", border: "1px solid #3b82f644", color: "#93c5fd",
                  fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                }}>🏢 Profil firme: {alert.company_name}</button>
              )}
              {alert.official_id && (
                <button onClick={() => onOpenEntity(alert.official_id, "Person", alert.official_name || alert.official_id)} style={{
                  flex: 1, padding: "9px 14px", borderRadius: 8, cursor: "pointer",
                  background: "#f59e0b22", border: "1px solid #f59e0b44", color: "#fbbf24",
                  fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                }}>👤 Profil: {alert.official_name}</button>
              )}
              {alert.institution_id && (
                <button onClick={() => onOpenEntity(alert.institution_id, "Institution", alert.institution || alert.institution_id)} style={{
                  flex: 1, padding: "9px 14px", borderRadius: 8, cursor: "pointer",
                  background: "#10b98122", border: "1px solid #10b98144", color: "#6ee7b7",
                  fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                }}>🏛 Profil: {(alert.institution || "").slice(0, 35)}{(alert.institution || "").length > 35 ? "…" : ""}</button>
              )}
            </div>
          )}

          {/* CTA */}
          <button onClick={onShowOnGraph} style={{
            padding: "11px 16px", borderRadius: 8, cursor: "pointer",
            background: `linear-gradient(135deg, ${color}30, ${color}18)`,
            border: `1px solid ${color}55`, color: "#f8fafc",
            fontSize: 13, fontWeight: 600,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
          }}>
            ◎ Prikaži na grafu
          </button>
        </div>
      </div>
    </div>
  );
}

function SectionTitle({ icon, title }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, textTransform: "uppercase",
      letterSpacing: "0.12em", color: "#475569", marginBottom: 10,
      fontFamily: "'IBM Plex Mono', monospace",
      display: "flex", alignItems: "center", gap: 6,
    }}>
      <span>{icon}</span>{title}
    </div>
  );
}

// ── Force Graph ─────────────────────────────────────────────────
function ForceGraph({ nodes, edges, onNodeClick, highlightIds }) {
  const svgRef = useRef(null);

  useEffect(() => {
    if (!svgRef.current || !nodes.length) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    const width = svgRef.current.clientWidth;
    const height = svgRef.current.clientHeight;
    const g = svg.append("g");

    const zoom = d3.zoom().scaleExtent([0.05, 8]).on("zoom", (e) => g.attr("transform", e.transform));
    svg.call(zoom);

    const defs = svg.append("defs");
    defs.append("marker").attr("id", "arrow").attr("viewBox", "0 -5 10 10")
      .attr("refX", 28).attr("refY", 0).attr("markerWidth", 6).attr("markerHeight", 6)
      .attr("orient", "auto").append("path").attr("d", "M0,-5L10,0L0,5").attr("fill", "#475569");
    const glow = defs.append("filter").attr("id", "glow");
    glow.append("feGaussianBlur").attr("stdDeviation", "3").attr("result", "blur");
    glow.append("feMerge").selectAll("feMergeNode")
      .data(["blur", "SourceGraphic"]).join("feMergeNode").attr("in", d => d);

    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const simNodes = nodes.map(n => ({ ...n }));
    const simEdges = edges
      .filter(e => nodeMap.has(e.source) && nodeMap.has(e.target))
      .map(e => ({ ...e }));

    const sim = d3.forceSimulation(simNodes)
      .force("link", d3.forceLink(simEdges).id(d => d.id).distance(120).strength(0.6))
      .force("charge", d3.forceManyBody().strength(-400))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(38))
      .force("x", d3.forceX(width / 2).strength(0.02))
      .force("y", d3.forceY(height / 2).strength(0.02));

    const link = g.append("g").selectAll("line").data(simEdges).join("line")
      .attr("stroke", d => d.relationship === "FAMILY_OF" ? "#f59e0b55" : d.relationship.includes("CONTRACT") ? "#ef444455" : "#334155")
      .attr("stroke-width", d => d.relationship.includes("CONTRACT") ? 2 : 1.5)
      .attr("stroke-opacity", 0.7)
      .attr("stroke-dasharray", d => d.relationship === "FAMILY_OF" ? "4,4" : null)
      .attr("marker-end", "url(#arrow)");

    const linkLabel = g.append("g").selectAll("text").data(simEdges).join("text")
      .text(d => d.relationship.replace(/_/g, " "))
      .attr("font-size", 7).attr("fill", "#334155").attr("text-anchor", "middle")
      .attr("font-family", "'IBM Plex Mono', monospace").attr("pointer-events", "none");

    const isHl = d => !highlightIds || highlightIds.has(d.id);
    const node = g.append("g").selectAll("g").data(simNodes).join("g")
      .style("cursor", "pointer")
      .call(d3.drag()
        .on("start", (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
        .on("end", (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on("click", (e, d) => onNodeClick?.(d));

    const isSeed = d => d.props?.source === 'seed';

    node.append("circle")
      .attr("r", d => d.type === "Institution" ? 20 : d.type === "Company" ? 17 : d.type === "Contract" ? 14 : 16)
      .attr("fill", d => ENTITY_COLORS[d.type] || "#6b7280")
      .attr("fill-opacity", d => isSeed(d) ? 0.4 : 1)
      .attr("stroke", d => {
        if (isSeed(d)) return "#f59e0b";
        return isHl(d) && highlightIds ? "#fff" : "transparent";
      })
      .attr("stroke-width", d => isSeed(d) ? 1.5 : (isHl(d) && highlightIds ? 3 : 0))
      .attr("stroke-dasharray", d => isSeed(d) ? "4,3" : null)
      .attr("opacity", d => isHl(d) ? 1 : 0.12)
      .attr("filter", d => isHl(d) && highlightIds ? "url(#glow)" : null);

    const typeIcons = { Person: "👤", Company: "🏢", Institution: "🏛", Contract: "📄", PoliticalParty: "⚑", BudgetItem: "💰", Property: "🏠" };
    node.append("text").text(d => typeIcons[d.type] || "?")
      .attr("dy", 5).attr("text-anchor", "middle").attr("font-size", 12).attr("pointer-events", "none");
    node.append("text")
      .text(d => d.name?.length > 22 ? d.name.slice(0, 20) + "…" : d.name)
      .attr("dy", 32).attr("text-anchor", "middle").attr("font-size", 10)
      .attr("fill", "#cbd5e1").attr("font-family", "'DM Sans', sans-serif")
      .attr("opacity", d => isHl(d) ? 1 : 0.12).attr("pointer-events", "none");

    // TEST badge for seed nodes
    node.filter(d => isSeed(d)).append("text")
      .text("TEST")
      .attr("dy", -22).attr("text-anchor", "middle")
      .attr("font-size", 6).attr("fill", "#f59e0b").attr("font-weight", 700)
      .attr("font-family", "'IBM Plex Mono', monospace").attr("pointer-events", "none");

    sim.on("tick", () => {
      link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
      linkLabel.attr("x", d => (d.source.x + d.target.x) / 2).attr("y", d => (d.source.y + d.target.y) / 2 - 4);
      node.attr("transform", d => `translate(${d.x},${d.y})`);
    });

    setTimeout(() => {
      svg.transition().duration(700).call(zoom.transform, d3.zoomIdentity.translate(width * 0.1, height * 0.1).scale(0.82));
    }, 900);

    return () => sim.stop();
  }, [nodes, edges, highlightIds, onNodeClick]);

  return <svg ref={svgRef} style={{ width: "100%", height: "100%", background: "transparent" }} />;
}

// ── Entity Browser Panel ────────────────────────────────────────
function EntityBrowser({ type, label, color, onClose, onSelectEntity, excludeSeed = false }) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(true);

  const TYPE_ICONS = { Person: "👤", Company: "🏢", Institution: "🏛", Contract: "📄", PoliticalParty: "⚑" };

  const load = useCallback(async (query = "") => {
    setLoading(true);
    const url = `/entities?type=${type}&limit=100${query ? `&q=${encodeURIComponent(query)}` : ""}${excludeSeed ? "&exclude_seed=true" : ""}`;
    const d = await apiFetch(url);
    if (d) {
      const rawItems = d.items || [];
      const filtered = excludeSeed ? rawItems.filter(it => it.props?.source !== 'seed') : rawItems;
      setItems(filtered);
      setTotal(excludeSeed ? filtered.length : (d.total || 0));
    }
    setLoading(false);
  }, [type, excludeSeed]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    const t = setTimeout(() => load(q), 250);
    return () => clearTimeout(t);
  }, [q, load]);

  const getSubtitle = (item) => {
    const p = item.props || {};
    if (type === "Person") return [p.current_role, p.party_name].filter(Boolean).join(" · ") || p.institution_name || "";
    if (type === "Company") return [p.status, p.city].filter(Boolean).join(" · ") || "";
    if (type === "Institution") return p.institution_type || "";
    if (type === "Contract") return p.value_rsd ? formatRSD(p.value_rsd) + (p.award_date ? ` · ${p.award_date}` : "") : "";
    if (type === "PoliticalParty") return p.abbreviation || "";
    return "";
  };

  return (
    <div style={{
      position: "fixed", inset: 0, zIndex: 900,
      display: "flex", justifyContent: "flex-start",
      background: "rgba(0,0,0,0.5)", backdropFilter: "blur(2px)",
    }} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div style={{
        width: "min(420px, 96vw)", background: "#0d1525",
        borderRight: "1px solid #1e293b", overflowY: "auto",
        display: "flex", flexDirection: "column",
        animation: "slideInLeft 0.2s ease-out",
      }}>
        <div style={{
          padding: "16px 18px 12px", borderBottom: "1px solid #1e293b",
          position: "sticky", top: 0, zIndex: 2,
          background: "#0d1525ee", borderTop: `3px solid ${color}`,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#f8fafc" }}>
              {TYPE_ICONS[type]} {label}
              <span style={{ marginLeft: 8, fontSize: 10, color: "#475569", fontFamily: "'IBM Plex Mono', monospace" }}>
                ({loading ? "…" : total})
              </span>
            </div>
            <button onClick={onClose} style={{
              background: "#1e293b", border: "1px solid #334155", color: "#94a3b8",
              width: 28, height: 28, borderRadius: 6, cursor: "pointer", fontSize: 16,
              display: "flex", alignItems: "center", justifyContent: "center",
            }}>×</button>
          </div>
          <input
            type="text" placeholder={`Pretraži ${label.toLowerCase()}…`} value={q}
            onChange={e => setQ(e.target.value)}
            autoFocus
            style={{
              width: "100%", background: "#111827", border: "1px solid #1e293b",
              borderRadius: 6, padding: "7px 11px", color: "#e2e8f0", fontSize: 12,
              outline: "none", fontFamily: "'DM Sans', sans-serif",
            }}
          />
        </div>

        <div style={{ flex: 1, padding: "8px 0" }}>
          {loading ? (
            <div style={{ padding: 24, display: "flex", justifyContent: "center" }}><Spinner /></div>
          ) : items.length === 0 ? (
            <div style={{ padding: 24, color: "#475569", fontSize: 12, textAlign: "center" }}>Nema rezultata</div>
          ) : items.map((item, i) => {
            const sub = getSubtitle(item);
            const verUrl = item.props?.verification_url;
            return (
              <div key={item.id || i} style={{
                padding: "10px 18px", cursor: "pointer",
                borderBottom: "1px solid #0d1525",
                transition: "background 0.1s",
                display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8,
              }}
                onMouseEnter={e => e.currentTarget.style.background = "#111827"}
                onMouseLeave={e => e.currentTarget.style.background = "transparent"}
              >
                <div style={{ flex: 1, minWidth: 0 }} onClick={() => { onClose(); onSelectEntity(item.id, type, item.name); }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: "#e2e8f0", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                    {item.name || item.id}
                  </div>
                  {sub && <div style={{ fontSize: 10, color: "#64748b", marginTop: 2, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{sub}</div>}
                </div>
                <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                  <button
                    title="Istraži u grafu"
                    onClick={() => { onClose(); onSelectEntity(item.id, type, item.name); }}
                    style={{ background: color + "22", border: `1px solid ${color}44`, color: "#e2e8f0", padding: "3px 8px", borderRadius: 4, fontSize: 9, cursor: "pointer", fontFamily: "'IBM Plex Mono', monospace", whiteSpace: "nowrap" }}
                  >Graf →</button>
                  {verUrl && (
                    <a href={verUrl} target="_blank" rel="noopener noreferrer" title="Proveri izvor"
                      onClick={e => e.stopPropagation()}
                      style={{ background: "#1e293b", border: "1px solid #334155", color: "#60a5fa", padding: "3px 7px", borderRadius: 4, fontSize: 9, cursor: "pointer", textDecoration: "none", display: "flex", alignItems: "center" }}>↗</a>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ── Entity Detail Modal ─────────────────────────────────────────
function EntityDetailModal({ entityId, entityType, entityName, onClose, onNavigate, onShowOnGraph }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setData(null);
    setLoading(true);
    let url;
    if (entityType === "Company") url = `/company/${encodeURIComponent(entityId)}`;
    else if (entityType === "Person") url = `/person/${encodeURIComponent(entityId)}`;
    else if (entityType === "Institution") url = `/institution/${encodeURIComponent(entityId)}`;
    else { setLoading(false); return; }
    apiFetch(url).then(d => { setData(d); setLoading(false); });
  }, [entityId, entityType]);

  const color = ENTITY_COLORS[entityType] || "#6b7280";
  const aprLink = entityType === "Company" && data?.company?.maticni_broj
    ? `https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=${data.company.maticni_broj}`
    : null;

  const renderCompany = () => {
    const co = data.company || {};
    const directors = [...new Map((data.directors || []).filter(d => d.name).map(d => [d.name, d])).values()];
    const contracts = (data.contracts || []).filter(c => c.title).sort((a, b) => (b.value || 0) - (a.value || 0));
    const totalValue = contracts.reduce((s, c) => s + (c.value || 0), 0);
    return (
      <>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
          {co.founding_date && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#94a3b8", fontFamily: "monospace" }}>📅 {co.founding_date}</span>}
          {co.activity_name && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#94a3b8" }}>{co.activity_name}</span>}
          {co.maticni_broj && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#64748b", fontFamily: "monospace" }}>MB: {co.maticni_broj}</span>}
          {co.pib && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#64748b", fontFamily: "monospace" }}>PIB: {co.pib}</span>}
        </div>

        {directors.length > 0 && (
          <section style={{ marginBottom: 20 }}>
            <SectionTitle icon="👤" title={`Direktori / vlasnici (${directors.length})`} />
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {directors.map((d, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 12px", background: "#111827", borderRadius: 6 }}>
                  <span style={{ fontSize: 12, color: "#f59e0b", fontWeight: 600 }}>{d.name}</span>
                  {d.id && <button onClick={() => onNavigate(d.id, "Person", d.name)} style={{ fontSize: 9, padding: "2px 7px", background: "#f59e0b22", border: "1px solid #f59e0b44", color: "#f59e0b", borderRadius: 4, cursor: "pointer", fontFamily: "monospace" }}>Profil →</button>}
                </div>
              ))}
            </div>
          </section>
        )}

        <section>
          <SectionTitle icon="📄" title={`Ugovori (${contracts.length}) — ukupno ${formatRSD(totalValue)}`} />
          <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 380, overflowY: "auto" }}>
            {contracts.map((c, i) => (
              <div key={i} style={{ padding: "8px 12px", background: "#111827", borderRadius: 6, display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 10 }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 11, color: "#e2e8f0", lineHeight: 1.5 }}>{c.title}</div>
                  {c.institution && <div style={{ fontSize: 10, color: "#10b981", marginTop: 2 }}>{c.institution}</div>}
                  {c.date && <div style={{ fontSize: 9, color: "#475569", marginTop: 1, fontFamily: "monospace" }}>{c.date}</div>}
                </div>
                <div style={{ flexShrink: 0, textAlign: "right" }}>
                  <div style={{ fontSize: 11, color: "#f59e0b", fontWeight: 700, fontFamily: "monospace" }}>{formatRSD(c.value)}</div>
                  <a href={`https://jnportal.ujn.gov.rs/tender-documents/${c.id?.replace("JNP-", "")}`} target="_blank" rel="noopener noreferrer" style={{ fontSize: 8, color: "#3b82f6", textDecoration: "none" }}>↗ JN</a>
                </div>
              </div>
            ))}
          </div>
        </section>
      </>
    );
  };

  const renderPerson = () => {
    const p = data.person || {};
    const companies = (data.outgoing || []).filter(r => r.name && r.type);
    const institutions = (data.incoming || []).filter(r => r.name && r.type);
    return (
      <>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
          {p.source && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#94a3b8", fontFamily: "monospace" }}>{p.source.toUpperCase()}</span>}
          {p.person_id && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#64748b", fontFamily: "monospace" }}>{p.person_id}</span>}
        </div>
        {companies.length > 0 && (
          <section style={{ marginBottom: 20 }}>
            <SectionTitle icon="🏢" title={`Veze ka firmama (${companies.length})`} />
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {companies.map((r, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 12px", background: "#111827", borderRadius: 6 }}>
                  <div>
                    <span style={{ fontSize: 11, color: "#3b82f6", fontWeight: 600 }}>{r.name}</span>
                    <span style={{ fontSize: 9, color: "#475569", marginLeft: 8, fontFamily: "monospace" }}>{r.type}</span>
                  </div>
                  {r.id && <button onClick={() => onNavigate(r.id, r.target_label || "Company", r.name)} style={{ fontSize: 9, padding: "2px 7px", background: "#3b82f622", border: "1px solid #3b82f644", color: "#3b82f6", borderRadius: 4, cursor: "pointer", fontFamily: "monospace" }}>Profil →</button>}
                </div>
              ))}
            </div>
          </section>
        )}
        {institutions.length > 0 && (
          <section>
            <SectionTitle icon="🏛" title={`Veze ka institucijama (${institutions.length})`} />
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {institutions.map((r, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 12px", background: "#111827", borderRadius: 6 }}>
                  <div>
                    <span style={{ fontSize: 11, color: "#10b981" }}>{r.name}</span>
                    <span style={{ fontSize: 9, color: "#475569", marginLeft: 8, fontFamily: "monospace" }}>{r.type}</span>
                  </div>
                  {r.id && <button onClick={() => onNavigate(r.id, r.source_label || "Institution", r.name)} style={{ fontSize: 9, padding: "2px 7px", background: "#10b98122", border: "1px solid #10b98144", color: "#10b981", borderRadius: 4, cursor: "pointer", fontFamily: "monospace" }}>Profil →</button>}
                </div>
              ))}
            </div>
          </section>
        )}
      </>
    );
  };

  const renderInstitution = () => {
    const inst = data.institution || {};
    const contracts = (data.contracts || []).filter(c => c.title).sort((a, b) => (b.value || 0) - (a.value || 0));
    const totalValue = contracts.reduce((s, c) => s + (c.value || 0), 0);
    const employees = (data.employees || []).filter(e => e.name);
    return (
      <>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
          {inst.maticni_broj && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#64748b", fontFamily: "monospace" }}>MB: {inst.maticni_broj}</span>}
          {inst.pib && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#1e293b", color: "#64748b", fontFamily: "monospace" }}>PIB: {inst.pib}</span>}
          {inst.verification_url && <a href={inst.verification_url} target="_blank" rel="noopener noreferrer" style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "#0d1f3c", color: "#60a5fa", border: "1px solid #1e3a5f", textDecoration: "none" }}>↗ UJN portal</a>}
        </div>
        {employees.length > 0 && (
          <section style={{ marginBottom: 20 }}>
            <SectionTitle icon="👤" title={`Zaposleni funkcioneri (${employees.length})`} />
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {employees.map((e, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 12px", background: "#111827", borderRadius: 6 }}>
                  <span style={{ fontSize: 11, color: "#f59e0b" }}>{e.name}</span>
                  {e.role && <span style={{ fontSize: 9, color: "#475569" }}>{e.role}</span>}
                </div>
              ))}
            </div>
          </section>
        )}
        <section>
          <SectionTitle icon="📄" title={`Javne nabavke (${contracts.length}) — ukupno ${formatRSD(totalValue)}`} />
          <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 380, overflowY: "auto" }}>
            {contracts.map((c, i) => (
              <div key={i} style={{ padding: "8px 12px", background: "#111827", borderRadius: 6, display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 10 }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 11, color: "#e2e8f0", lineHeight: 1.5 }}>{c.title}</div>
                  {c.winner && <div style={{ fontSize: 10, color: "#3b82f6", marginTop: 2 }}>{c.winner}</div>}
                  {c.date && <div style={{ fontSize: 9, color: "#475569", marginTop: 1, fontFamily: "monospace" }}>{c.date}</div>}
                </div>
                <div style={{ flexShrink: 0 }}>
                  <div style={{ fontSize: 11, color: "#f59e0b", fontWeight: 700, fontFamily: "monospace" }}>{formatRSD(c.value)}</div>
                </div>
              </div>
            ))}
          </div>
        </section>
      </>
    );
  };

  const TYPE_ICONS = { Company: "🏢", Person: "👤", Institution: "🏛" };

  return (
    <div onClick={e => e.target === e.currentTarget && onClose()} style={{
      position: "fixed", inset: 0, zIndex: 1100,
      display: "flex", justifyContent: "flex-end",
      background: "rgba(0,0,0,0.65)", backdropFilter: "blur(3px)",
    }}>
      <div style={{
        width: "min(620px, 97vw)", background: "#0d1525",
        borderLeft: "1px solid #1e293b", overflowY: "auto",
        display: "flex", flexDirection: "column",
        animation: "slideInRight 0.2s ease-out",
      }}>
        {/* Header */}
        <div style={{
          padding: "18px 22px 14px", borderBottom: "1px solid #1e293b",
          position: "sticky", top: 0, zIndex: 2,
          background: "#0d1525ee", backdropFilter: "blur(8px)",
          borderTop: `3px solid ${color}`,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
              <span style={{ fontSize: 22 }}>{TYPE_ICONS[entityType] || "◎"}</span>
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: 9, color: color, fontWeight: 700, textTransform: "uppercase", fontFamily: "monospace", letterSpacing: "0.1em" }}>{entityType}</div>
                <div style={{ fontSize: 15, fontWeight: 700, color: "#f8fafc", letterSpacing: "-0.02em", wordBreak: "break-word" }}>{entityName}</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
              {onShowOnGraph && (
                <button onClick={onShowOnGraph} style={{ padding: "5px 10px", borderRadius: 6, cursor: "pointer", background: "#1e293b", border: "1px solid #334155", color: "#94a3b8", fontSize: 10, fontFamily: "monospace" }}>◎ Graf</button>
              )}
              {aprLink && (
                <a href={aprLink} target="_blank" rel="noopener noreferrer" style={{ padding: "5px 10px", borderRadius: 6, background: "#0d1f3c", border: "1px solid #1e3a5f", color: "#60a5fa", fontSize: 10, fontFamily: "monospace", textDecoration: "none" }}>↗ APR</a>
              )}
              <button onClick={onClose} style={{ background: "#1e293b", border: "1px solid #334155", color: "#94a3b8", width: 30, height: 30, borderRadius: 6, cursor: "pointer", fontSize: 18, display: "flex", alignItems: "center", justifyContent: "center" }}>×</button>
            </div>
          </div>
        </div>

        <div style={{ padding: "20px 22px", display: "flex", flexDirection: "column", gap: 0 }}>
          {loading ? (
            <div style={{ padding: 40, display: "flex", justifyContent: "center" }}><Spinner size={28} /></div>
          ) : !data ? (
            <div style={{ color: "#475569", fontSize: 13, textAlign: "center", padding: 40 }}>Nema podataka</div>
          ) : entityType === "Company" ? renderCompany()
            : entityType === "Person" ? renderPerson()
            : entityType === "Institution" ? renderInstitution()
            : null}
        </div>
      </div>
    </div>
  );
}

// ── Main Dashboard ──────────────────────────────────────────────
export default function Dashboard() {
  const [activeTab, setActiveTab] = useState("graph");
  const [showTestData, setShowTestData] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [highlightIds, setHighlightIds] = useState(null);
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [detailAlert, setDetailAlert] = useState(null);
  const [isDemo, setIsDemo] = useState(true);
  const [loading, setLoading] = useState(true);
  const [entityBrowser, setEntityBrowser] = useState(null); // { type, label, color }
  const [detailEntity, setDetailEntity] = useState(null); // { id, type, name }

  const [nodes, setNodes] = useState(DEMO_NODES);
  const [edges, setEdges] = useState(DEMO_EDGES);
  const [alerts, setAlerts] = useState(DEMO_ALERTS);
  const [stats, setStats] = useState(null);
  const [flaggedCounts, setFlaggedCounts] = useState(null); // counts from /graph/suspicious
  const [sourceCounts, setSourceCounts] = useState({});
  const [riskSummary, setRiskSummary] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function init() {
      setLoading(true);
      const statsData = await apiFetch("/stats");
      if (!cancelled && statsData && (statsData.total_persons > 0 || statsData.total_companies > 0)) {
        setStats(statsData);
        setIsDemo(false);

        // Load source counts, SUSPICIOUS graph only, and detections in parallel
        const [srcData, graphData, detectData] = await Promise.all([
          apiFetch("/stats/sources"),
          apiFetch("/graph/suspicious?limit=300"),
          apiFetch("/detect/all"),
        ]);

        if (!cancelled) {
          if (srcData?.by_source) setSourceCounts(srcData.by_source);
          if (graphData) {
            // Store flagged entity counts for sidebar (these are the only counts that matter)
            if (graphData.flagged_counts) setFlaggedCounts(graphData.flagged_counts);
            if (graphData.nodes?.length) {
              setNodes(graphData.nodes);
              setEdges(graphData.edges || []);
            }
          }
          if (detectData) {
            setRiskSummary(detectData.risk_summary);
            const allAlerts = [];
            for (const [name, data] of Object.entries(detectData.detections || {})) {
              for (const p of (data.patterns || [])) {
                allAlerts.push({ ...p, pattern_type: p.pattern_type || name });
              }
            }
            allAlerts.sort((a, b) => (SEVERITY_ORDER[a.severity] || 3) - (SEVERITY_ORDER[b.severity] || 3));
            if (allAlerts.length > 0) setAlerts(allAlerts);
          }
        }
      } else {
        if (!cancelled) {
          setStats({
            total_persons: DEMO_NODES.filter(n => n.type === "Person").length,
            total_companies: DEMO_NODES.filter(n => n.type === "Company").length,
            total_contracts: DEMO_NODES.filter(n => n.type === "Contract").length,
            total_institutions: DEMO_NODES.filter(n => n.type === "Institution").length,
            total_relationships: DEMO_EDGES.length,
          });
        }
      }
      if (!cancelled) setLoading(false);
    }
    init();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (searchQuery.length < 2 || isDemo) { setSearchResults([]); return; }
    const t = setTimeout(async () => {
      setIsSearching(true);
      const d = await apiFetch(`/search?q=${encodeURIComponent(searchQuery)}&limit=10`);
      if (d?.results) setSearchResults(d.results);
      setIsSearching(false);
    }, 300);
    return () => clearTimeout(t);
  }, [searchQuery, isDemo]);

  const exploreEntity = useCallback(async (id, type) => {
    if (isDemo) return;
    setLoading(true);
    const d = await apiFetch(`/graph/neighborhood?entity_id=${encodeURIComponent(id)}&entity_type=${type}&depth=2`);
    if (d?.nodes?.length) { setNodes(d.nodes); setEdges(d.edges || []); setActiveTab("graph"); setSearchResults([]); setSearchQuery(""); }
    setLoading(false);
  }, [isDemo]);

  const handleAlertClick = useCallback((alert) => {
    setDetailAlert(alert);
  }, []);

  const handleShowOnGraph = useCallback((alert) => {
    setDetailAlert(null);
    setSelectedAlert(alert);
    const ids = alert.entities ||
      [alert.official_id, alert.family_id, alert.company_mb, alert.contract_id,
       alert.person_id, alert.institution_id, alert.winner_mb].filter(Boolean);
    setHighlightIds(new Set(ids));
    setActiveTab("graph");
  }, []);

  const handleNodeClick = useCallback((node) => {
    setSelectedNode(node);
    setDetailAlert(null);
    const connected = new Set([node.id]);
    edges.forEach(e => {
      const s = typeof e.source === "string" ? e.source : e.source?.id;
      const t = typeof e.target === "string" ? e.target : e.target?.id;
      if (s === node.id || t === node.id) { connected.add(s); connected.add(t); }
    });
    setHighlightIds(connected);
  }, [edges]);

  const clearSelection = () => {
    setSelectedNode(null); setSelectedAlert(null); setDetailAlert(null); setHighlightIds(null);
  };

  const isSeedAlert = a => {
    const ids = [a.company_mb, a.company_id, a.official_id, a.person_id, a.institution_id,
                 a.id_1, a.id_2, a.institution_1_id, a.institution_2_id].filter(Boolean).join(" ");
    return ids.includes("SEED") || ids.includes("seed");
  };

  const filteredAlerts = useMemo(() =>
    alerts.filter(a => {
      if (!showTestData && isSeedAlert(a)) return false;
      return !searchQuery ||
        Object.values(a).join(" ").toLowerCase().includes(searchQuery.toLowerCase()) ||
        (PATTERN_LABELS[a.pattern_type] || "").toLowerCase().includes(searchQuery.toLowerCase());
    }), [alerts, searchQuery, showTestData]);

  const visibleNodes = useMemo(() =>
    showTestData ? nodes : nodes.filter(n => n.props?.source !== 'seed'),
    [nodes, showTestData]);

  const visibleEdges = useMemo(() => {
    if (showTestData) return edges;
    const visibleIds = new Set(visibleNodes.map(n => n.id));
    return edges.filter(e => visibleIds.has(e.source) && visibleIds.has(e.target));
  }, [edges, visibleNodes, showTestData]);

  const realNodeCount = useMemo(() => nodes.filter(n => n.props?.source !== 'seed').length, [nodes]);
  const testNodeCount = useMemo(() => nodes.filter(n => n.props?.source === 'seed').length, [nodes]);

  // Honest source registry — what data is actually in the DB and where it came from
  const SOURCE_REGISTRY = [
    {
      key: "jnportal",
      name: "JN Portal — Portal javnih nabavki",
      description: "Pravi ugovori o javnim nabavkama sa jnportal.ujn.gov.rs — naziv pobednika, PIB kompanije, vrednost ugovora, datum. 1.3M+ ugovora ukupno; skupljaju se top ugovori sortirani po vrednosti.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://jnportal.ujn.gov.rs/contracts",
      urlLabel: "jnportal.ujn.gov.rs",
      countLabel: "čvorova",
    },
    {
      key: "ujn",
      name: "UJN OpenData — istorijski tenderi",
      description: "Istorijski podaci o javnim nabavkama sa portala Uprave za javne nabavke: institucija, vrednost, vrsta postupka, datum. Pokriva 2020. godinu i starije periode.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://jnportal.ujn.gov.rs/tender-documents/search",
      urlLabel: "jnportal.ujn.gov.rs",
      countLabel: "zapisa",
    },
    {
      key: "op",
      name: "Otvoreni Parlament — poslanici i imovinske karte",
      description: "Profili svih aktuelnih narodnih poslanika sa otvoreniparlament.rs: stranka, imenovanje u odborima, imovinska karta (nekretnine, vozila, kompanije). Ključni izvor za detekciju konflikta interesa — poslanik kao direktor firme koja dobija javne ugovore.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://otvoreniparlament.rs/poslanik",
      urlLabel: "otvoreniparlament.rs",
      countLabel: "poslanika",
    },
    {
      key: "rik",
      name: "Narodna skupština RS — poslanici (rezervni)",
      description: "Rezervna lista poslanika sa parlament.gov.rs korišćena kao fallback kada primarni skupljač ne može da dohvati podatke. Otvoreni Parlament je primarni izvor.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://www.parlament.gov.rs/members-of-parliament",
      urlLabel: "parlament.gov.rs",
      countLabel: "poslanika",
    },
    {
      key: "apr",
      name: "APR — Agencija za privredne registre",
      description: "Registar privrednih subjekata: vlasnici, direktori, matični brojevi, adrese, PIB. Podaci se preuzimaju sa companywall.rs koji agregira javni APR registar. Pokriva privatne kompanije (državna preduzeća nisu na companywall.rs).",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://pretraga.apr.gov.rs",
      urlLabel: "pretraga.apr.gov.rs",
      countLabel: "firmi",
    },
    {
      key: "opendata",
      name: "data.gov.rs — Registar stranaka",
      description: "Registar aktivnih i istorijskih političkih stranaka sa data.gov.rs: naziv, predsednik, registarski broj, adresa, datum osnivanja.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://data.gov.rs/sr/datasets/politichke-stranke/",
      urlLabel: "data.gov.rs",
      countLabel: "stranaka",
    },
    {
      key: "rgz",
      name: "RGZ — Katastar nekretnina",
      description: "Vlasništvo nad nekretninama za privredne subjekte. Katastar parcele preuzimaju se iz companywall.rs koji integriše RGZ podatke.",
      badge: "AKTIVAN",
      badgeColor: "#10b981",
      url: "https://rgz.gov.rs/usluge/eLine",
      urlLabel: "rgz.gov.rs",
      countLabel: "nekretnina",
    },
    {
      key: "gazette",
      name: "Službeni glasnik RS",
      description: "Rešenja o postavljenjima i razrešenjima funkcionera objavljena u Službenom glasniku RS. Skupljač parsira pravno-informacioni-sistem.rs.",
      badge: "DELIMIČNO",
      badgeColor: "#f59e0b",
      url: "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection",
      urlLabel: "pravno-informacioni-sistem.rs",
      countLabel: "rešenja",
    },
    {
      key: "seed",
      name: "Sintetički test podaci",
      description: "Veštački podaci generisani za demonstraciju obrazaca korupcije. Služe isključivo za testiranje — čvorovi su vizuelno označeni isprekidanim okvirom i natpisom TEST. Uključeni su u grafičke prikaze samo kada je aktiviran toggle TEST.",
      badge: "TEST",
      badgeColor: "#f59e0b",
      url: null,
      countLabel: "čvorova",
    },
  ];

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
        @keyframes spin { to { transform: rotate(360deg) } }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(6px) } to { opacity: 1; transform: translateY(0) } }
        @keyframes slideInRight { from { transform: translateX(40px); opacity: 0 } to { transform: translateX(0); opacity: 1 } }
        @keyframes slideInLeft { from { transform: translateX(-40px); opacity: 0 } to { transform: translateX(0); opacity: 1 } }
        @keyframes pulse { 0%, 100% { opacity: 1 } 50% { opacity: 0.4 } }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
      `}</style>

      {entityBrowser && (
        <EntityBrowser
          type={entityBrowser.type}
          label={entityBrowser.label}
          color={entityBrowser.color}
          onClose={() => setEntityBrowser(null)}
          excludeSeed={!showTestData}
          onSelectEntity={(id, type, name) => {
            exploreEntity(id, type);
          }}
        />
      )}

      {detailAlert && (
        <PatternDetailModal
          alert={detailAlert}
          onClose={() => setDetailAlert(null)}
          onShowOnGraph={() => handleShowOnGraph(detailAlert)}
          onOpenEntity={(id, type, name) => { setDetailAlert(null); setDetailEntity({ id, type, name }); }}
        />
      )}

      {detailEntity && (
        <EntityDetailModal
          entityId={detailEntity.id}
          entityType={detailEntity.type}
          entityName={detailEntity.name}
          onClose={() => setDetailEntity(null)}
          onNavigate={(id, type, name) => setDetailEntity({ id, type, name })}
          onShowOnGraph={() => { setDetailEntity(null); exploreEntity(detailEntity.id, detailEntity.type); setActiveTab("graph"); }}
        />
      )}

      <div style={{
        width: "100vw", height: "100vh", background: "#080c15",
        color: "#e2e8f0", fontFamily: "'DM Sans', system-ui, sans-serif",
        display: "flex", flexDirection: "column", overflow: "hidden",
      }}>
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
              <div style={{ fontWeight: 600, fontSize: 15, letterSpacing: "-0.02em" }}>Srpska Transparentnost</div>
              <div style={{ fontSize: 10, color: "#475569", fontFamily: "'IBM Plex Mono', monospace", display: "flex", alignItems: "center", gap: 6 }}>
                <span>GRAPH INTELLIGENCE</span>
                {isDemo && <span style={{ fontSize: 8, background: "#f5970b22", color: "#f59e0b", padding: "1px 6px", borderRadius: 3, fontWeight: 600 }}>DEMO</span>}
              </div>
            </div>
          </div>

          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <div style={{ position: "relative" }}>
              <input type="text" placeholder="Pretraži entitete..." value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                style={{
                  background: "#131b2e", border: "1px solid #1e293b", borderRadius: 6,
                  padding: "6px 12px 6px 30px", color: "#e2e8f0", fontSize: 13, width: 240,
                  outline: "none", fontFamily: "'DM Sans', sans-serif",
                }}
                onFocus={e => e.target.style.borderColor = "#3b82f6"}
                onBlur={e => e.target.style.borderColor = "#1e293b"}
              />
              <span style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)", fontSize: 13, color: "#475569" }}>⌕</span>
              {isSearching && <div style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)" }}><Spinner size={14} /></div>}
              {searchResults.length > 0 && (
                <div style={{
                  position: "absolute", top: "100%", left: 0, right: 0,
                  background: "#131b2e", border: "1px solid #1e293b",
                  borderRadius: 6, marginTop: 4, zIndex: 200,
                  boxShadow: "0 8px 24px #00000088", maxHeight: 280, overflowY: "auto",
                }}>
                  {searchResults.map((r, i) => (
                    <div key={i} onClick={() => exploreEntity(r.id, r.type)} style={{
                      padding: "8px 12px", cursor: "pointer",
                      borderBottom: i < searchResults.length - 1 ? "1px solid #1e293b" : "none",
                    }}
                      onMouseEnter={e => e.currentTarget.style.background = "#1e293b"}
                      onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                    >
                      <div style={{ fontSize: 12, fontWeight: 500 }}>{r.name}</div>
                      <div style={{ fontSize: 10, color: ENTITY_COLORS[r.type] || "#64748b", fontFamily: "'IBM Plex Mono', monospace" }}>
                        {r.type}{r.role ? ` • ${r.role}` : ""}{r.status ? ` • ${r.status}` : ""}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div style={{ background: "#131b2e", borderRadius: 6, display: "flex", border: "1px solid #1e293b", overflow: "hidden" }}>
              {[{ key: "graph", label: "Graf", icon: "◎" }, { key: "alerts", label: "Upozorenja", icon: "⚠" }, { key: "data", label: "Podaci", icon: "◈" }].map(({ key, label, icon }) => (
                <button key={key} onClick={() => setActiveTab(key)} style={{
                  padding: "6px 14px", fontSize: 11, fontWeight: 500,
                  background: activeTab === key ? "#1e293b" : "transparent",
                  color: activeTab === key ? "#f8fafc" : "#64748b",
                  border: "none", cursor: "pointer",
                  letterSpacing: "0.02em", fontFamily: "'IBM Plex Mono', monospace",
                  display: "flex", alignItems: "center", gap: 4,
                }}>
                  <span style={{ fontSize: 10 }}>{icon}</span> {label}
                  {key === "alerts" && alerts.length > 0 && (
                    <span style={{
                      fontSize: 8, background: SEVERITY_COLORS[riskSummary?.risk_level || "low"] + "33",
                      color: SEVERITY_COLORS[riskSummary?.risk_level || "low"],
                      padding: "1px 5px", borderRadius: 3, fontWeight: 700,
                    }}>{alerts.length}</span>
                  )}
                </button>
              ))}
            </div>

            <button
              onClick={() => setShowTestData(s => !s)}
              title={showTestData ? "Sakrij TEST/seed podatke" : "Prikaži TEST/seed podatke"}
              style={{
                padding: "5px 10px", fontSize: 10, fontWeight: 600,
                background: showTestData ? "#f59e0b22" : "#1e293b",
                border: `1px solid ${showTestData ? "#f59e0b55" : "#334155"}`,
                color: showTestData ? "#f59e0b" : "#64748b",
                borderRadius: 5, cursor: "pointer",
                fontFamily: "'IBM Plex Mono', monospace",
                letterSpacing: "0.04em", whiteSpace: "nowrap",
              }}
            >
              {showTestData ? "◉ TEST" : "◌ TEST"}
            </button>
          </div>
        </header>

        <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
          {/* Sidebar */}
          <aside style={{ width: 270, borderRight: "1px solid #151d2e", padding: 14, overflowY: "auto", flexShrink: 0, background: "#0a1020" }}>
            <div style={{ marginBottom: 18 }}>
              <div style={{ fontSize: 9, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.12em", color: "#475569", marginBottom: 4, fontFamily: "'IBM Plex Mono', monospace", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span>Pregled baze</span>
                <div style={{ display: "flex", gap: 3 }}>
                  {!isDemo && realNodeCount > 0 && (
                    <span style={{ fontSize: 7, color: "#10b981", background: "#10b98115", padding: "1px 4px", borderRadius: 3, fontWeight: 700, border: "1px solid #10b98133" }}>◉ REAL</span>
                  )}
                  {!isDemo && testNodeCount > 0 && showTestData && (
                    <span style={{ fontSize: 7, color: "#f59e0b", background: "#f59e0b15", padding: "1px 4px", borderRadius: 3, fontWeight: 700, border: "1px solid #f59e0b33" }}>◌ TEST</span>
                  )}
                  {!isDemo && flaggedCounts != null && (
                    <span style={{ fontSize: 7, color: "#dc2626", background: "#dc262615", padding: "1px 4px", borderRadius: 3, fontWeight: 700 }}>⚠</span>
                  )}
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
                {[
                  { label: "Osobe", flagged: flaggedCounts?.persons, total: stats?.total_persons, color: ENTITY_COLORS.Person, type: "Person" },
                  { label: "Firme", flagged: flaggedCounts?.companies, total: stats?.total_companies, color: ENTITY_COLORS.Company, type: "Company" },
                  { label: "Ugovori", flagged: flaggedCounts?.contracts, total: stats?.total_contracts, color: ENTITY_COLORS.Contract, type: "Contract" },
                  { label: "Institucije", flagged: flaggedCounts?.institutions, total: stats?.total_institutions, color: ENTITY_COLORS.Institution, type: "Institution" },
                ].map(s => {
                  // Primary value: flagged count if available and real data loaded, else total
                  const primaryVal = (!isDemo && flaggedCounts != null) ? (s.flagged ?? 0) : s.total;
                  const showTotal = showTestData && !isDemo && flaggedCounts != null && s.total != null && s.total !== primaryVal;
                  return (
                  <div key={s.label}
                    onClick={() => !isDemo && setEntityBrowser({ type: s.type, label: s.label, color: s.color })}
                    style={{
                      background: "#111827", borderRadius: 8, padding: "8px 10px", borderLeft: `3px solid ${s.color}`,
                      animation: loading ? "pulse 1.5s infinite" : "none",
                      cursor: isDemo ? "default" : "pointer",
                      transition: "background 0.15s",
                      position: "relative",
                    }}
                    onMouseEnter={e => { if (!isDemo) e.currentTarget.style.background = "#161f30"; }}
                    onMouseLeave={e => e.currentTarget.style.background = "#111827"}
                  >
                    <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "'IBM Plex Mono', monospace", color: s.color }}>{loading ? "…" : (primaryVal ?? "—")}</div>
                    <div style={{ fontSize: 9, color: "#64748b", marginTop: 2, display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
                      <span>{s.label}{!isDemo && flaggedCounts != null ? " ⚠" : ""}</span>
                      {showTotal && <span style={{ color: "#1e3a5f", fontSize: 8 }}>/{s.total} ukupno</span>}
                      {!showTotal && !isDemo && <span style={{ color: "#334155" }}>↗</span>}
                    </div>
                  </div>
                  );
                })}
              </div>

              <div style={{ background: "#111827", borderRadius: 8, padding: "10px 12px", marginTop: 6, borderLeft: `3px solid ${SEVERITY_COLORS[riskSummary?.risk_level || "low"]}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: SEVERITY_COLORS[riskSummary?.risk_level || "low"], fontFamily: "'IBM Plex Mono', monospace" }}>{filteredAlerts.length}</div>
                    <div style={{ fontSize: 9, color: "#64748b" }}>Upozorenja</div>
                  </div>
                  {riskSummary && (
                    <div style={{ textAlign: "right" }}>
                      <RiskBadge level={riskSummary.risk_level} />
                      <div style={{ fontSize: 9, color: "#64748b", marginTop: 3 }}>Rizik nivo</div>
                    </div>
                  )}
                </div>
                {riskSummary?.severity_counts && (
                  <div style={{ display: "flex", gap: 8, marginTop: 8, flexWrap: "wrap" }}>
                    {Object.entries(riskSummary.severity_counts).filter(([, v]) => v > 0).map(([sev, count]) => (
                      <span key={sev} style={{ fontSize: 9, fontFamily: "'IBM Plex Mono', monospace", color: SEVERITY_COLORS[sev] }}>{count}× {sev}</span>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div style={{ marginBottom: 18 }}>
              <div style={{ fontSize: 9, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.12em", color: "#475569", marginBottom: 8, fontFamily: "'IBM Plex Mono', monospace" }}>Legenda</div>
              {Object.entries(ENTITY_COLORS).map(([type, color]) => (
                <div key={type} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
                  <div style={{ width: 8, height: 8, borderRadius: "50%", background: color, flexShrink: 0 }} />
                  <span style={{ fontSize: 11, color: "#94a3b8" }}>
                    {{ Person: "Osoba", Company: "Firma", Institution: "Institucija", Contract: "Ugovor", PoliticalParty: "Stranka", Address: "Adresa", BudgetItem: "Budžet", Property: "Nekretnina" }[type] || type}
                  </span>
                </div>
              ))}
            </div>

            {selectedNode && (
              <div style={{ background: "#111827", borderRadius: 8, padding: 12, border: `1px solid ${ENTITY_COLORS[selectedNode.type] || "#1e293b"}`, animation: "fadeIn 0.2s ease-out" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                  <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em", color: ENTITY_COLORS[selectedNode.type], fontWeight: 600, fontFamily: "'IBM Plex Mono', monospace", display: "flex", alignItems: "center", gap: 6 }}>
                    {{ Person: "Osoba", Company: "Firma", Institution: "Institucija", Contract: "Ugovor", PoliticalParty: "Stranka" }[selectedNode.type] || selectedNode.type}
                    {selectedNode.props?.source && (
                      <span style={{
                        fontSize: 7, padding: "1px 5px", borderRadius: 3, fontWeight: 700,
                        background: selectedNode.props.source === 'seed' ? "#f59e0b22" : "#10b98122",
                        color: selectedNode.props.source === 'seed' ? "#f59e0b" : "#10b981",
                        border: `1px solid ${selectedNode.props.source === 'seed' ? "#f59e0b55" : "#10b98155"}`,
                      }}>{selectedNode.props.source === 'seed' ? 'TEST' : selectedNode.props.source.toUpperCase()}</span>
                    )}
                  </div>
                  <button onClick={clearSelection} style={{ background: "none", border: "none", color: "#475569", cursor: "pointer", fontSize: 16, lineHeight: 1, padding: 0 }}>×</button>
                </div>
                <div style={{ fontSize: 13, fontWeight: 600, marginTop: 5 }}>{selectedNode.name}</div>
                {selectedNode.props?.current_role && <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 3 }}>Pozicija: {selectedNode.props.current_role}</div>}
                {selectedNode.props?.value_rsd && <div style={{ fontSize: 10, color: "#f59e0b", marginTop: 3, fontFamily: "'IBM Plex Mono', monospace" }}>{formatRSD(selectedNode.props.value_rsd)}</div>}
                {selectedNode.props?.status && <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 3 }}>Status: {selectedNode.props.status}</div>}
                {!isDemo && (
                  <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
                    <button onClick={() => setDetailEntity({ id: selectedNode.id, type: selectedNode.type, name: selectedNode.name })} style={{
                      flex: 1, background: (ENTITY_COLORS[selectedNode.type] || "#3b82f6") + "22",
                      border: `1px solid ${(ENTITY_COLORS[selectedNode.type] || "#3b82f6")}44`,
                      color: "#e2e8f0", padding: "4px 10px", borderRadius: 4,
                      fontSize: 10, cursor: "pointer", fontFamily: "'IBM Plex Mono', monospace",
                    }}>◈ Profil</button>
                    <button onClick={() => exploreEntity(selectedNode.id, selectedNode.type)} style={{
                      flex: 1, background: "#1e293b",
                      border: "1px solid #334155",
                      color: "#94a3b8", padding: "4px 10px", borderRadius: 4,
                      fontSize: 10, cursor: "pointer", fontFamily: "'IBM Plex Mono', monospace",
                    }}>◎ Graf</button>
                  </div>
                )}
              </div>
            )}

            {selectedAlert && !selectedNode && (
              <div style={{ background: "#111827", borderRadius: 8, padding: 12, border: `1px solid ${SEVERITY_COLORS[selectedAlert.severity]}44`, animation: "fadeIn 0.2s ease-out" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: 9, textTransform: "uppercase", color: SEVERITY_COLORS[selectedAlert.severity], fontWeight: 700, fontFamily: "'IBM Plex Mono', monospace" }}>
                    {PATTERN_LABELS[selectedAlert.pattern_type] || selectedAlert.pattern_type}
                  </span>
                  <RiskBadge level={selectedAlert.severity} />
                </div>
                <div style={{ fontSize: 11, marginTop: 8, color: "#94a3b8" }}>Entiteti označeni na grafu.</div>
                <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
                  <button onClick={() => setDetailAlert(selectedAlert)} style={{ flex: 1, background: "#1e293b", border: "1px solid #334155", color: "#e2e8f0", padding: "4px 8px", borderRadius: 4, fontSize: 10, cursor: "pointer" }}>Detalji</button>
                  <button onClick={clearSelection} style={{ background: "none", border: "1px solid #1e293b", color: "#475569", padding: "4px 8px", borderRadius: 4, fontSize: 10, cursor: "pointer" }}>×</button>
                </div>
              </div>
            )}
          </aside>

          {/* Main content */}
          <main style={{ flex: 1, position: "relative", overflow: "hidden" }}>
            {activeTab === "graph" && (
              <ForceGraph nodes={visibleNodes} edges={visibleEdges} onNodeClick={handleNodeClick} highlightIds={highlightIds} />
            )}

            {activeTab === "alerts" && (
              <div style={{ padding: 20, overflowY: "auto", height: "100%" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
                  <div style={{ fontSize: 9, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.12em", color: "#475569", fontFamily: "'IBM Plex Mono', monospace" }}>
                    Detektovani obrasci ({filteredAlerts.length})
                  </div>
                  {!isDemo && (
                    <div style={{ display: "flex", gap: 6 }}>
                      {[
                        { label: "CSV", format: "csv", title: "Preuzmi CSV" },
                        { label: "JSON", format: "json", title: "Preuzmi JSON" },
                        { label: "HTML izveštaj", format: "html", title: "Otvori printabilni izveštaj" },
                      ].map(({ label, format, title }) => (
                        <a key={format}
                          href={`${API_BASE}/export/findings?format=${format}${!showTestData ? "&exclude_seed=true" : ""}`}
                          target={format === "html" ? "_blank" : "_self"}
                          download={format !== "html" ? undefined : undefined}
                          rel="noopener noreferrer"
                          title={title}
                          style={{
                            fontSize: 9, padding: "4px 10px", borderRadius: 4, cursor: "pointer",
                            fontFamily: "'IBM Plex Mono', monospace", fontWeight: 600,
                            background: "#1e293b", border: "1px solid #334155",
                            color: "#94a3b8", textDecoration: "none",
                            display: "flex", alignItems: "center", gap: 4,
                            transition: "all 0.15s",
                          }}
                          onMouseEnter={e => { e.currentTarget.style.background = "#334155"; e.currentTarget.style.color = "#e2e8f0"; }}
                          onMouseLeave={e => { e.currentTarget.style.background = "#1e293b"; e.currentTarget.style.color = "#94a3b8"; }}
                        >↓ {label}</a>
                      ))}
                    </div>
                  )}
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {filteredAlerts.map((alert, i) => {
                    const exp = PATTERN_EXPLANATIONS[alert.pattern_type] || {};
                    const color = SEVERITY_COLORS[alert.severity];
                    const moneyVal = alert.contract_value || alert.value_rsd || alert.total_value || alert.company_total_value || alert.donation_amount;
                    const isTestData = [alert.official_id, alert.family_id, alert.company_mb, alert.contract_id,
                      alert.person_id, alert.institution_id, alert.winner_mb].some(id => id && String(id).includes('SEED'));
                    return (
                      <div key={i} onClick={() => handleAlertClick(alert)} style={{
                        background: "#111827", borderRadius: 8, padding: "14px 16px",
                        borderLeft: `4px solid ${color}`,
                        cursor: "pointer", transition: "all 0.15s",
                        animation: `fadeIn 0.3s ease-out ${i * 0.04}s both`,
                        opacity: isTestData ? 0.75 : 1,
                      }}
                        onMouseEnter={e => e.currentTarget.style.background = "#161f30"}
                        onMouseLeave={e => e.currentTarget.style.background = "#111827"}
                      >
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                            <span style={{ fontSize: 16 }}>{exp.icon || "⚠"}</span>
                            <span style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color, fontFamily: "'IBM Plex Mono', monospace" }}>
                              {PATTERN_LABELS[alert.pattern_type] || alert.pattern_type}
                            </span>
                            {isTestData && (
                              <span style={{ fontSize: 7, padding: "1px 5px", borderRadius: 3, fontWeight: 700, fontFamily: "'IBM Plex Mono', monospace", background: "#f59e0b22", color: "#f59e0b", border: "1px solid #f59e0b44" }}>TEST</span>
                            )}
                          </div>
                          <RiskBadge level={alert.severity} />
                        </div>

                        {/* Key evidence fields as a compact summary */}
                        <div style={{ fontSize: 12, color: "#cbd5e1", lineHeight: 1.6 }}>
                          {alert.official_name && <span><strong style={{ color: "#f59e0b" }}>{alert.official_name}</strong> ({alert.institution}) → </span>}
                          {alert.family_member && <span>porodica: <strong style={{ color: "#f59e0b" }}>{alert.family_member}</strong> → </span>}
                          {alert.company_name && <span>firma: <strong style={{ color: "#3b82f6" }}>{alert.company_name}</strong></span>}
                          {alert.person_name && !alert.official_name && <span><strong style={{ color: "#f59e0b" }}>{alert.person_name}</strong></span>}
                          {alert.winner && <span> → <strong style={{ color: "#3b82f6" }}>{alert.winner}</strong></span>}
                          {alert.contract_title && <span> → ugovor: <em style={{ color: "#ef4444" }}>{alert.contract_title}</em></span>}
                          {alert.donor_company && <span><strong style={{ color: "#3b82f6" }}>{alert.donor_company}</strong> → <strong style={{ color: "#8b5cf6" }}>{alert.party_name}</strong> → {alert.awarding_institution}</span>}
                          {alert.address && <span>Adresa: <strong>{alert.address}</strong> ({alert.num_companies} firmi)</span>}
                          {alert.name_1 && <span><strong>{alert.name_1}</strong> u {alert.institution_1} i {alert.institution_2}</span>}
                          {alert.pattern_type === 'repeated_winner' && alert.institution && (
                            <span> — <strong style={{ color: "#10b981" }}>{alert.institution}</strong> ({alert.num_contracts}× ugovora)</span>
                          )}
                          {alert.pattern_type === 'institutional_monopoly' && alert.institution && (
                            <span><strong style={{ color: "#10b981" }}>{alert.institution}</strong> → <strong style={{ color: "#3b82f6" }}>{alert.company_name}</strong>{alert.company_pct_of_institution != null ? ` (${alert.company_pct_of_institution}%)` : ""}</span>
                          )}
                          {alert.pattern_type === 'new_company_big_contract' && alert.age_at_award != null && (
                            <span> — osnovana {alert.founded}, ugovor {alert.age_at_award === 0 ? "iste godine" : `za ${alert.age_at_award} god.`}{alert.num_contracts > 1 ? `, ${alert.num_contracts} ugovora` : ""}</span>
                          )}
                          {alert.pattern_type === 'samododeljivanje_proxy' && alert.official_name && (
                            <span><strong style={{ color: "#f87171" }}>{alert.official_name}</strong> → direktor: <strong style={{ color: "#3b82f6" }}>{alert.company_name}</strong></span>
                          )}
                          {alert.pattern_type === 'direct_official_contractor' && alert.official_name && (
                            <span><strong style={{ color: "#f87171" }}>{alert.official_name}</strong> ({alert.institution}) → <strong style={{ color: "#3b82f6" }}>{alert.company_name}</strong></span>
                          )}
                        </div>

                        {moneyVal && (
                          <div style={{ fontSize: 11, color: "#f59e0b", marginTop: 6, fontFamily: "'IBM Plex Mono', monospace", fontWeight: 700 }}>
                            {formatRSD(moneyVal)}
                          </div>
                        )}

                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 8 }}>
                          <div style={{ fontSize: 9, color: "#3b82f688", fontFamily: "'IBM Plex Mono', monospace" }}>◉ Klikni za detalje, objašnjenje i izvore</div>
                          {alert.company_mb && (
                            <button onClick={e => { e.stopPropagation(); setDetailEntity({ id: alert.company_mb, type: "Company", name: alert.company_name || alert.company_mb }); }} style={{
                              fontSize: 9, padding: "2px 8px", borderRadius: 4, cursor: "pointer",
                              background: "#3b82f622", border: "1px solid #3b82f644", color: "#93c5fd",
                              fontFamily: "'IBM Plex Mono', monospace",
                            }}>🏢 Profil firme</button>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {activeTab === "data" && (
              <div style={{ padding: 20, overflowY: "auto", height: "100%" }}>

                {/* Data provenance notice */}
                {sourceCounts["seed"] > 0 && (sourceCounts["jnportal"] > 0 || sourceCounts["ujn"] > 0) && (
                  <div style={{
                    background: "#0a1a0f", border: "1px solid #10b98133", borderRadius: 8,
                    padding: "12px 14px", marginBottom: 18,
                    display: "flex", gap: 10, alignItems: "flex-start",
                  }}>
                    <span style={{ fontSize: 16, flexShrink: 0 }}>◉</span>
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: "#10b981", marginBottom: 3 }}>Graf sadrži mešavinu realnih i test podataka</div>
                      <div style={{ fontSize: 11, color: "#6ee7b7", lineHeight: 1.6 }}>
                        <strong style={{ color: "#10b981" }}>Realni čvorovi</strong> (puni, bez isprekidanog okvira) potiču iz JN Portala i UJN OpenData.<br />
                        <strong style={{ color: "#f59e0b" }}>Test čvorovi</strong> (isprekidani zlatni okvir, natpis TEST) su sintetički demonstracioni podaci.<br />
                        Upozorenja označena sa <span style={{ background: "#f59e0b22", color: "#f59e0b", padding: "0 4px", borderRadius: 2, fontSize: 9, fontFamily: "monospace" }}>TEST</span> uključuju isključivo veštačke entitete.
                      </div>
                    </div>
                  </div>
                )}
                {sourceCounts["seed"] > 0 && !sourceCounts["jnportal"] && !sourceCounts["ujn"] && !sourceCounts["apr"] && (
                  <div style={{
                    background: "#1a1208", border: "1px solid #f59e0b44", borderRadius: 8,
                    padding: "12px 14px", marginBottom: 18,
                    display: "flex", gap: 10, alignItems: "flex-start",
                  }}>
                    <span style={{ fontSize: 16, flexShrink: 0 }}>⚠</span>
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: "#f59e0b", marginBottom: 3 }}>Baza sadrži samo sintetičke podatke</div>
                      <div style={{ fontSize: 11, color: "#92400e", lineHeight: 1.6 }}>
                        Svi čvorovi u grafu su veštački kreirani za demonstraciju. Pravi podaci se skupljaju pokretanjem <code style={{ background: "#231908", padding: "1px 4px", borderRadius: 3, fontFamily: "monospace" }}>POST /ingest/jnportal</code>.
                      </div>
                    </div>
                  </div>
                )}

                <div style={{ fontSize: 9, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.12em", color: "#475569", marginBottom: 12, fontFamily: "'IBM Plex Mono', monospace" }}>
                  Registrovani izvori podataka
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 28 }}>
                  {SOURCE_REGISTRY.filter(src => showTestData || src.key !== "seed").map((src, i) => {
                    const nodeCount = sourceCounts[src.key] ?? 0;
                    const hasData = nodeCount > 0;
                    return (
                      <div key={i} style={{
                        background: "#111827", borderRadius: 8, padding: "14px 16px",
                        border: `1px solid ${hasData ? (src.key === "seed" ? "#f59e0b33" : "#10b98133") : "#1e293b"}`,
                        animation: `fadeIn 0.3s ease-out ${i * 0.06}s both`,
                      }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 6 }}>
                          <div style={{ fontSize: 12, fontWeight: 700, color: "#e2e8f0", flex: 1 }}>{src.name}</div>
                          <span style={{
                            fontSize: 8, padding: "2px 7px", borderRadius: 3, fontWeight: 700,
                            fontFamily: "'IBM Plex Mono', monospace", flexShrink: 0, marginLeft: 8,
                            background: hasData ? src.badgeColor + "22" : "#1e293b",
                            color: hasData ? src.badgeColor : "#334155",
                            border: `1px solid ${hasData ? src.badgeColor + "55" : "#1e293b"}`,
                          }}>
                            {hasData ? (src.key === "seed" ? "TEST" : "AKTIVAN") : src.badge}
                          </span>
                        </div>
                        <div style={{ fontSize: 11, color: "#64748b", lineHeight: 1.65, marginBottom: 8 }}>{src.description}</div>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                          {src.url ? (
                            <a href={src.url} target="_blank" rel="noopener noreferrer" style={{
                              fontSize: 10, color: "#3b82f6", fontFamily: "'IBM Plex Mono', monospace",
                              textDecoration: "none", display: "flex", alignItems: "center", gap: 4,
                            }}
                              onMouseEnter={e => e.currentTarget.style.color = "#60a5fa"}
                              onMouseLeave={e => e.currentTarget.style.color = "#3b82f6"}
                            >↗ {src.urlLabel}</a>
                          ) : <span />}
                          <span style={{ fontSize: 10, fontFamily: "'IBM Plex Mono', monospace", color: hasData ? "#94a3b8" : "#334155" }}>
                            {hasData ? `${nodeCount} ${src.countLabel}` : "—"}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>

                <div style={{ fontSize: 9, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.12em", color: "#475569", marginBottom: 12, fontFamily: "'IBM Plex Mono', monospace" }}>
                  Detekcioni obrasci
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {Object.entries(PATTERN_EXPLANATIONS).map(([key, exp], i) => {
                    const count = alerts.filter(a => a.pattern_type === key).length;
                    return (
                      <div key={key}
                        onClick={() => count > 0 && setActiveTab("alerts")}
                        style={{
                          background: "#111827", borderRadius: 6, padding: "9px 12px",
                          borderLeft: `2px solid ${count > 0 ? SEVERITY_COLORS.high : "#1e293b"}`,
                          animation: `fadeIn 0.3s ease-out ${i * 0.05}s both`,
                          display: "flex", justifyContent: "space-between", alignItems: "center",
                          cursor: count > 0 ? "pointer" : "default",
                          transition: "background 0.1s",
                        }}
                        onMouseEnter={e => { if (count > 0) e.currentTarget.style.background = "#161f30"; }}
                        onMouseLeave={e => e.currentTarget.style.background = "#111827"}
                      >
                        <div>
                          <span style={{ fontSize: 13 }}>{exp.icon}</span>
                          <span style={{ fontSize: 11, fontFamily: "'IBM Plex Mono', monospace", color: count > 0 ? "#3b82f6" : "#334155", marginLeft: 6 }}>{exp.title}</span>
                        </div>
                        {count > 0
                          ? <span style={{ fontSize: 10, color: SEVERITY_COLORS.high, fontFamily: "'IBM Plex Mono', monospace", fontWeight: 700 }}>{count} pogodak{count > 1 ? "a" : ""} →</span>
                          : <span style={{ fontSize: 9, color: "#334155", fontFamily: "'IBM Plex Mono', monospace" }}>0</span>
                        }
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {activeTab === "graph" && (
              <div style={{
                position: "absolute", bottom: 14, left: 14,
                background: "#0a1020ee", borderRadius: 8, padding: "6px 12px",
                border: "1px solid #151d2e", fontSize: 9,
                fontFamily: "'IBM Plex Mono', monospace", color: "#475569",
                display: "flex", gap: 10, alignItems: "center",
              }}>
                {realNodeCount > 0 && (
                  <span style={{ color: "#10b981" }}>◉ {realNodeCount} realni</span>
                )}
                {realNodeCount > 0 && testNodeCount > 0 && showTestData && <span>+</span>}
                {testNodeCount > 0 && showTestData && (
                  <span style={{ color: "#f59e0b" }}>◌ {testNodeCount} test</span>
                )}
                {nodes.length > 0 && <><span>·</span><span>{edges.length} veza</span></>}
                <span>·</span>
                <span>Klikni čvor za detalje</span>
                {isDemo && <><span>·</span><span style={{ color: "#f59e0b" }}>Demo podaci</span></>}
              </div>
            )}
          </main>
        </div>
      </div>
    </>
  );
}
