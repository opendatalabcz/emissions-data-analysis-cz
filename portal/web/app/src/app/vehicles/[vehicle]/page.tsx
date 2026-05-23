"use client";

import Link from "next/link";
import useSWR from "swr";
import BreadcrumbsContainer from "@/components/BreadcrumbsContainer";
import Container from "@/components/Container";
import SearchBox from "../SearchBox";
import Breadcrumb from "antd/es/breadcrumb";
import Spin from "antd/es/spin";
import Card from "antd/es/card";
import Tag from "antd/es/tag";

export default function VehiclePredictionPage({
  params: { vehicle },
}: {
  params: { vehicle: string };
}) {
  const { data, isLoading } = useSWR(
    `/api/predict?vin=${vehicle}`,
    async (key) => {
      const res = await fetch(key);
      return await res.json();
    },
    { revalidateOnFocus: false, revalidateOnReconnect: false }
  );

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center grow space-y-4 pt-12">
        <Spin size="large" />
        <p className="text-gray-500">Provádím výpočet pomocí modelu strojového učení, může to chvíli trvat...</p>
      </div>
    );
  }
  
  if (!data || data.error) {
    return (
      <div className="flex flex-col items-center justify-center space-y-8 grow pt-12">
        <p className="text-red-500">{data?.error || "Vozidlo nebylo nalezeno nebo nastala neočekávaná chyba."}</p>
        <SearchBox initialValue={vehicle}></SearchBox>
        <Link href={"/"} className="text-primary">
          Zpět na vyhledávání vozidla
        </Link>
      </div>
    );
  }

  const riskGroup = data.Skupina;
  let color = "green";
  if (riskGroup >= 5) color = "red";
  else if (riskGroup >= 3) color = "orange";

  return (
    <>
      <BreadcrumbsContainer>
        <Breadcrumb
          items={[
            { title: <Link href="/">SME portál</Link> },
            { title: data.vin },
          ]}
        ></Breadcrumb>
      </BreadcrumbsContainer>

      <Container>
        <h1 className="pb-2 text-3xl">
          {data.make} {data.model}
        </h1>
        
        <p className="pb-8 text-gray-600 leading-relaxed">
          První registrace: <span className="font-semibold">{data.first_registration ? new Date(data.first_registration).toLocaleDateString("cs-CZ") : "Neznámá"}</span> <br/>
          Poslední úspěšné měření: <span className="font-semibold">{data.last_inspection ? new Date(data.last_inspection).toLocaleDateString("cs-CZ") : "Neznámá"}</span> <br/>
          VIN: <span className="font-mono bg-gray-100 px-2 py-0.5 rounded text-sm">{data.vin}</span>
        </p>

        <Card title="Výsledek predikce rizikovosti" className="max-w-2xl mb-8 border-gray-200 shadow-sm">
          <div className="flex flex-col space-y-4">
            <div className="flex justify-between items-center border-b pb-4">
              <span className="text-lg">Kategorie rizikovosti (1-4):</span>
              <Tag color={color} className="text-2xl px-4 py-1 m-0">{data.Skupina}</Tag>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-lg">Průměrná neúspěšnost této kategorie:</span>
              <span className="text-xl font-semibold">{(data.Prumerna_Neuspesnost_Skupiny * 100).toFixed(1)} %</span>
            </div>
          </div>
        </Card>

      </Container>
    </>
  );
}
