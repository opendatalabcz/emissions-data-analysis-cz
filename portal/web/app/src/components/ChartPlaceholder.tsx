'use client';

import { useEffect, useState } from "react";
import Spin from "antd/es/spin";

interface ChartPlaceholderProps {
  filename: string;
  altText: string;
}

export default function ChartPlaceholder({ filename, altText }: ChartPlaceholderProps) {
  const [retryHash, setRetryHash] = useState<string>('');
  const [hasError, setHasError] = useState(false);

  useEffect(() => {
    setHasError(false);
  }, [filename]);

  return (
    <div className="flex items-center justify-center h-full grow min-h-[400px] rounded-lg shadow-sm border border-slate-200 bg-white p-4 relative">
      {hasError ? (
        <div className="flex flex-col items-center justify-center text-gray-500">
          <p className="mb-2">Graf zatím není k dispozici nebo se jej nepodařilo načíst.</p>
          <button 
            className="px-4 py-2 mt-2 bg-primary text-white rounded-md hover:bg-red-600 transition-colors"
            onClick={() => { setHasError(false); setRetryHash(`&v=${Date.now()}`); }}
          >
            Zkusit znovu
          </button>
        </div>
      ) : (
        <img 
          src={`/api/graphs_svg?file=${filename}${retryHash}`} 
          alt={altText}
          className="w-full max-w-5xl object-contain"
          onError={() => setHasError(true)}
        />
      )}
    </div>
  );
}
