import Container from "@/components/Container";
import BreadcrumbsContainer from "@/components/BreadcrumbsContainer";
import Link from "next/link";
import Breadcrumb from "antd/es/breadcrumb";

export default async function AboutPage() {
  return (
    <>
      <BreadcrumbsContainer>
        <Breadcrumb
          items={[
            { title: <Link href="/">SME portál</Link> },
            { title: "O projektu" },
          ]}
        ></Breadcrumb>
      </BreadcrumbsContainer>

      <Container>
        <h1 className="pb-4 text-3xl">O projektu</h1>
        <p className="pb-4">
          SME Portál je analytický a informační web, který vznikl jako bakalářská práce na{" "}
          <a href="https://fit.cvut.cz/" target="_blank" className="text-blue-600 hover:underline">Fakultě informačních technologií ČVUT v Praze</a>{" "}
          pod záštitou laboratoře otevřených dat{" "}
          <a href="https://opendatalab.cz/" target="_blank" className="text-blue-600 hover:underline">OpenDataLab</a>. 
          Tento projekt navazuje na webovou aplikaci <a href="https://stk.opendatalab.cz/" target="_blank" className="text-blue-600 hover:underline">stk.opendatalab.cz</a>, jejímž autorem je <a href="https://www.linkedin.com/in/daniel-brotz/" target="_blank" className="text-blue-600 hover:underline">Daniel Brotz</a>.
        </p>

        <h2 className="self-start pt-6 pb-4 text-2xl font-semibold">Otevřená data a automatické aktualizace</h2>
        <p className="pb-4">
          Veškeré zobrazované statistiky a grafy jsou tvořeny na základě dat pocházejících z <strong>Informačního systému Ministerstva dopravy ČR</strong>. Aplikace je navržena tak, aby se pravidelně a zcela automaticky aktualizovala. Díky tomu portál vždy reflektuje nejnovější stav a poskytuje aktuální informace. 
        </p>
        <p className="pb-4">
          Konkrétně projekt integruje následující datové sady evidované v Národním katalogu otevřených dat (NKOD):
        </p>
        <ul className="pb-4 pl-6 list-disc space-y-2">
          <li>
            <a href="https://data.gov.cz/datov%C3%A1-sada?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F66003008%2Fe8e07fa264f3bd2179be03381ec324de" target="_blank" className="text-blue-600 hover:underline">Data z měřicích přístrojů získaných při měření emisí</a> (zdroj pro interaktivní a časové grafy)
          </li>
          <li>
            <a href="https://data.gov.cz/datov%C3%A1-sada?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F66003008%2F9c95ebdba1dc7a2fbcfc5b6c07d25705" target="_blank" className="text-blue-600 hover:underline">Prohlídky vozidel STK a SME</a> (společně s daty z přístrojů tvoří informační základnu pro predikční model)
          </li>
        </ul>

        <h2 className="self-start pt-6 pb-4 text-2xl font-semibold">Predikční model rizikovosti</h2>
        <p className="pb-4">
          Součástí portálu je také model strojového učení, jehož cílem je odhadnout míru rizika neúspěchu konkrétního vozidla na nadcházející technické kontrole. Tento model pro své predikci využívá <strong>veškeré evidované informace o vozidle získané během jeho poslední úspěšné technické prohlídky a měření emisí</strong>. Propojením identifikačních znaků a naměřených fyzikálních veličin je poté schopen odhadnout rizikovost příští kontroly emisí.
        </p>
        <p className="pb-4">
          Vozidla jsou modelem rozdělena do 4 kategorií podle odhadovaného rizika:
        </p>
        <ul className="pb-4 pl-6 list-disc space-y-2">
          <li><strong>Kategorie 1 (Nejnižší riziko):</strong> Neúspěšnost cca 0,5 % (tvoří zhruba 42 % vozidel)</li>
          <li><strong>Kategorie 2:</strong> Neúspěšnost cca 2,0 % (tvoří zhruba 30 % vozidel)</li>
          <li><strong>Kategorie 3:</strong> Neúspěšnost cca 5,0 % (tvoří zhruba 21 % vozidel)</li>
          <li><strong>Kategorie 4 (Nejvyšší riziko):</strong> Neúspěšnost cca 11,9 % (tvoří zhruba 7 % vozidel)</li>
        </ul>

        <h2 className="self-start pt-6 pb-4 text-2xl font-semibold">Metodika měření emisí</h2>
        <p className="pb-4">
          Samotný proces měření emisí na stanicích (SME) je přísně standardizován a řídí se sekvencí dílčích postupů. Tyto úkony se liší v závislosti na typu motoru (zážehový vs. vznětový) a technologické úrovni jeho emisního systému (neřízený, řízený, řízený s OBD diagnostikou).
        </p>
        <p className="pb-4">
          U moderních vozidel s <strong>řízeným emisním systémem</strong> zahrnuje měření typicky následující kroky:
        </p>
        <ul className="pb-4 pl-6 list-disc space-y-2">
          <li><strong>Identifikace vozidla a vizuální kontrola:</strong> Posouzení těsnosti a úplnosti výfukového traktu a přítomnosti komponent omezujících emise.</li>
          <li><strong>Diagnostika systému řízení motoru (OBD):</strong> Zjištění stavu varovné kontrolky MIL, vyčtení paměti emisně relevantních závad a zejména kontrola takzvaných kódů <em>readiness</em>. Ty indikují, zda palubní řídicí jednotka úspěšně prošla vlastními interními testy subsystémů.</li>
          <li><strong>Měření u zážehových (benzínových) motorů:</strong> Zjišťují se koncentrace oxidu uhelnatého (CO) při stanovených zvýšených a volnoběžných otáčkách motoru. Zároveň je při zvýšených otáčkách měřen součinitel přebytku vzduchu (tzv. hodnota λ) indikující zda se v motoru při spalování nachází optimální množství vzduchu.</li>
          <li><strong>Měření u vznětových (naftových) motorů:</strong> Sleduje se míra kouřivosti, otáčky motoru za volnoběhu a plném plynu spolu s dobou, za kterou byly maximální otáčky dosaženy. Měření probíhá formou volné akcelerace do oněch maximálních otáček (před aktivací omezovače), přičemž se z vícero opakování počítá průměrná hodnota.</li>
        </ul>

      </Container>
    </>
  );
}
