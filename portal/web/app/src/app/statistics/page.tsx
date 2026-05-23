"use client";

import Image from "next/image";
import Footer from "@/components/Footer";
import Header from "@/components/Header";
import Layout, { Content } from "antd/es/layout/layout";
import theme from "../themeConfig";
import Container from "@/components/Container";
import Link from "next/link";
import ConfigProvider from "antd/es/config-provider";
import ChartPlaceholder from "@/components/ChartPlaceholder";
import DistributionSliderChart from "@/components/DistributionSliderChart";

export default function StatisticsPage() {
  return (
    <ConfigProvider theme={theme}>
      <Layout>
        <Header></Header>
        <Content className="flex flex-col items-stretch w-full mx-auto lg:w-10/12 2xl:w-8/12">
          <Container>
            <div className="flex flex-col">
              <div className="flex flex-row items-center self-center py-6 space-x-6">
                <Image
                  src="/logo.svg"
                  alt="logo"
                  width="64"
                  height="64"
                  className="self-center"
                ></Image>
                <h1 className="text-4xl font-bold">Statistiky SME</h1>
              </div>

              <p className="leading-relaxed">
                Vítejte na stránce statistik SME portálu. Naleznete zde vizualizace aktuálních dat z měření emisí vozidel.
              </p>

              <h2 className="self-start pt-12 text-3xl">Celková úspěšnost</h2>
              <div className="w-full py-4 mb-8">
                <section className="space-y-4 mb-8">
                  <h3 className="text-xl font-semibold text-slate-700">Vývoj celkové průchodnosti</h3>
                  <p className="text-slate-600">Graf zobrazuje celkový procentuální podíl vozidel v ČR, která u měření emisí nevyhoví na kontrolách emisí.</p>
                  <ChartPlaceholder 
                    filename="vyvoj_pruchodnosti.svg" 
                    altText="Vývoj celkového podílu vozidel, která měření absolvují neúspěšně" 
                  />
                </section>
              </div>

              <h2 className="self-start pt-6 text-3xl">Časová náročnost měření</h2>
              <div className="w-full py-4 mb-8">
                <section className="space-y-4 mb-8">
                  <h3 className="text-xl font-semibold text-slate-700">Délka měření</h3>
                  <p className="text-slate-600">Vizualizace času potřebného k provedení kontroly od jejího zahájení do ukončení. Křivky ukazují běžnou délku měření (medián) a časy nejrychlejších kontrol (nejkratších 10&nbsp;%, 1&nbsp;% a 0,1&nbsp;% měření).</p>
                  <ChartPlaceholder 
                    filename="delka_prohlidky.svg" 
                    altText="Rozložení délky měření emisí v čase (kvantily)" 
                  />
                </section>
              </div>

              <h2 className="self-start pt-6 text-3xl">Vývoj hraničních a mezních hodnot</h2>
              <p className="pt-4 text-slate-600">
                Přehled měření, u kterých se zaznamenané hodnoty nacházejí na okraji nebo mimo předepsané tolerance, avšak celkový výsledek zkoušky je hodnocen jako vyhovující. U části vozidel se hodnoty mohou na hranici intervalu vyskytovat přirozeně. Detaily tohoto rozložení lze vidět <Link href="#rozlozeni-parametru" className="text-blue-600 hover:underline">zde</Link> v grafech na konci stránky.
              </p>
              <div className="w-full py-4 mb-8">
                <section className="space-y-4 mb-8">
                  <h3 className="text-xl font-semibold text-slate-700">Celkové anomálie u měření</h3>
                  <p className="text-slate-600">Sleduje procento úspěšně ukončených kontrol, u kterých systém eviduje data ležící zcela mimo povolené rozmezí.</p>
                  <ChartPlaceholder 
                    filename="mereni_anomalie_celkove.svg" 
                    altText="Podíl úspěšných měření s hodnotami mimo povolený rozsah" 
                  />
                </section>
                <section className="space-y-4 mb-8">
                  <h3 className="text-xl font-semibold text-slate-700">Krajní hodnoty: Otáčky</h3>
                  <p className="text-slate-600">Časová řada ukazující, jak často se výsledné otáčky motoru (u benzinu a nafty) shodují přesně s hraniční hodnotou povoleného limitu.</p>
                  <ChartPlaceholder 
                    filename="mereni_krajni_hodnoty_otacky.svg" 
                    altText="Podíl měření otáček na hranici povoleného intervalu" 
                  />
                </section>
                <section className="space-y-4 mb-8">
                  <h3 className="text-xl font-semibold text-slate-700">Krajní hodnoty: Akcelerace</h3>
                  <p className="text-slate-600">Časová řada ukazující frekvenci výskytu času akcelerace (u naftových motorů) přesně na limitní hranici.</p>
                  <ChartPlaceholder 
                    filename="mereni_krajni_hodnoty_akcelerace.svg" 
                    altText="Podíl času akcelerace na hranici intervalu" 
                  />
                </section>
              </div>

              <h2 id="rozlozeni-parametru" className="self-start pt-6 text-3xl">Detailní rozložení parametrů (Měsíční přehledy)</h2>
              <div className="w-full py-4 mb-8">
                <p className="pb-4 text-slate-600">
                  Tato sekce nabízí pohled na detailní rozložení naměřených fyzikálních veličin u všech úspěšně ukončených kontrol v rámci zvoleného měsíce. Zobrazená data zahrnují parametry povinné pro moderní benzínová a naftová vozidla. Pro snadné srovnání jsou hodnoty převedeny na jednotnou stupnici, kde 0 představuje spodní povolenou hranici a 1 horní limit (vozidla homologovaná podle různých norem mají odlišné limity).
                </p>
                <DistributionSliderChart />
              </div>

            </div>
          </Container>
        </Content>
        <Footer></Footer>
      </Layout>
    </ConfigProvider>
  );
}