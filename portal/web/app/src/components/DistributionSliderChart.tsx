"use client";

import { useEffect, useState } from "react";
import Slider from "antd/es/slider";
import Select from "antd/es/select";
import Spin from "antd/es/spin";
import ChartPlaceholder from "./ChartPlaceholder";

const NORM_METRICS = [
  { value: 'Benzin_OtackyVolnobezne_CO_Hodnota_Norm', label: 'Benzín: Emise oxidu uhelnatého (volnoběh)' },
  { value: 'Benzin_OtackyVolnobezne_N_Hodnota_Norm', label: 'Benzín: Otáčky motoru (volnoběh)' },
  { value: 'Benzin_OtackyZvysene_LAMBDA_Hodnota_Norm', label: 'Benzín: Kvalita spalování - Lambda (zvýšené otáčky)' },
  { value: 'Benzin_OtackyZvysene_CO_Hodnota_Norm', label: 'Benzín: Emise oxidu uhelnatého (zvýšené otáčky)' },
  { value: 'Benzin_OtackyZvysene_N_Hodnota_Norm', label: 'Benzín: Otáčky motoru (zvýšené otáčky)' },
  { value: 'Nafta_MereniPrumer_CasAkcelerace_Hodnota_Norm', label: 'Nafta: Doba vytočení motoru do maxima' },
  { value: 'Nafta_MereniPrumer_Kourivost_Hodnota_Norm', label: 'Nafta: Množství sazí ve výfuku (kouřivost)' },
  { value: 'Nafta_MereniPrumer_OtackyVolnobezne_Hodnota_Norm', label: 'Nafta: Otáčky motoru (volnoběh)' },
  { value: 'Nafta_MereniPrumer_OtackyPrebehove_Hodnota_Norm', label: 'Nafta: Maximální otáčky (při plném plynu)' }
];

export default function DistributionSliderChart() {
  const [months, setMonths] = useState<string[]>([]);
  const [selectedMonthIdx, setSelectedMonthIdx] = useState<number>(0);
  const [selectedMetric, setSelectedMetric] = useState<string>(NORM_METRICS[0].value);
  const [isLoading, setIsLoading] = useState(true);

  const fetchMonths = () => {
    setIsLoading(true);
    // Přidáno ?v=Date.now() pro ochranu proti cachování chyby 404 prohlížečem
    fetch('/api/graphs_json?v=' + Date.now())
      .then(res => {
        if (!res.ok) throw new Error("File not found");
        return res.json();
      })
      .then((data: string[]) => {
        setMonths(data);
        if (data.length > 0) {
          setSelectedMonthIdx(data.length - 1);
        }
        setIsLoading(false);
      })
      .catch(err => {
        console.error("Failed to load available months", err);
        setMonths([]);
        setIsLoading(false);
      });
  };

  useEffect(() => {
    fetchMonths();
  }, []);

  // Chytré přednačítání (preloading) obrázků pouze pro aktuálně zvolenou metriku
  useEffect(() => {
    if (months.length === 0) return;
    
    const preloadImages = () => {
      months.forEach((monthStr) => {
        const [y, m] = monthStr.split('-');
        const img = new Image();
        img.src = `/api/graphs_svg?file=rozdeleni_${selectedMetric}_${y}_${m}.svg`;
      });
    };

    // Spustíme přednačítání s mírným zpožděním, abychom neblokovali prvotní načtení stránky
    const timeoutId = setTimeout(preloadImages, 500);
    return () => clearTimeout(timeoutId);
  }, [months, selectedMetric]);

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-48 border border-slate-200 rounded-lg bg-white">
        <Spin size="large" />
      </div>
    );
  }

  if (months.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-48 text-gray-500 border border-slate-200 rounded-lg bg-white">
        <p className="mb-2">Grafy zatím nejsou k dispozici nebo se je nepodařilo načíst.</p>
        <button 
          className="px-4 py-2 mt-2 bg-primary text-white rounded-md hover:bg-red-600 transition-colors"
          onClick={fetchMonths}
        >
          Zkusit znovu
        </button>
      </div>
    );
  }

  const currentMonthStr = months[selectedMonthIdx];
  const [year, month] = currentMonthStr.split('-');
  const filename = `rozdeleni_${selectedMetric}_${year}_${month}.svg`;
  
  const selectedMetricLabel = NORM_METRICS.find(m => m.value === selectedMetric)?.label || selectedMetric;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center">
        <div className="w-full sm:w-1/3">
          <label className="block text-sm font-medium text-gray-700 mb-1">Měřená veličina</label>
          <Select 
            value={selectedMetric}
            onChange={setSelectedMetric}
            options={NORM_METRICS}
            className="w-full"
          />
        </div>
        <div className="w-full sm:w-2/3">
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Měsíc: <span className="font-semibold">{month}/{year}</span>
          </label>
          <Slider 
            min={0}
            max={months.length - 1}
            value={selectedMonthIdx}
            onChange={setSelectedMonthIdx}
            tooltip={{ formatter: (val) => months[val as number] }}
          />
        </div>
      </div>
      
      <div className="w-full">
        <ChartPlaceholder 
          filename={filename} 
          altText={`Rozložení normovaných hodnot - ${selectedMetricLabel}`} 
        />
      </div>
    </div>
  );
}