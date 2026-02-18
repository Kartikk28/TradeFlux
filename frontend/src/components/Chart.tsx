
import { useEffect, useRef } from 'react'
import { createChart, ColorType, IChartApi, CandlestickData, LineData } from 'lightweight-charts'

type Candle = { ts: string, open:number, high:number, low:number, close:number, volume:number }

export default function Chart({ candles, ema }: { candles: Candle[], ema?: { ts: string, value: number }[] }) {
  const ref = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)

  useEffect(() => {
    if (!ref.current) return
    const chart = createChart(ref.current, { height: 420, layout: { background: { type: ColorType.Solid, color: 'transparent' }} })
    const candleSeries = chart.addCandlestickSeries()
    candleSeries.setData(candles.map(c => ({ time: Date.parse(c.ts)/1000, open:c.open, high:c.high, low:c.low, close:c.close }) as CandlestickData))
    if (ema && ema.length) {
      const line = chart.addLineSeries({ lineWidth: 2 })
      line.setData(ema.map(x => ({ time: Date.parse(x.ts)/1000, value: x.value } as LineData)))
    }
    chart.timeScale().fitContent()
    chartRef.current = chart
    return () => { chart.remove() }
  }, [candles, ema])

  return <div className="card chart" ref={ref} />
}
