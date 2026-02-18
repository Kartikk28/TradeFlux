import React, { useEffect, useMemo, useRef } from "react"
import {
  createChart,
  CrosshairMode,
  LineStyle,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts"

type Candle = { ts: string; open: number; high: number; low: number; close: number; volume: number }

function isoToUTCTimestamp(iso: string): UTCTimestamp {
  const ms = Date.parse(iso)
  return Math.floor(ms / 1000) as UTCTimestamp
}

function ema(values: number[], period: number) {
  if (values.length === 0) return []
  const k = 2 / (period + 1)
  const out: number[] = []
  let prev = values[0]
  out.push(prev)
  for (let i = 1; i < values.length; i++) {
    const v = values[i]
    prev = v * k + prev * (1 - k)
    out.push(prev)
  }
  return out
}

export default function CandleChart({
  candles,
  showEMA = true,
  emaPeriod = 20,
}: {
  candles: Candle[]
  showEMA?: boolean
  emaPeriod?: number
}) {
  const elRef = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null)
  const emaSeriesRef = useRef<ISeriesApi<"Line"> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null)

  const data = useMemo(() => {
    return candles.map(c => ({
      time: isoToUTCTimestamp(c.ts),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }))
  }, [candles])

  const vol = useMemo(() => {
    return candles.map(c => ({
      time: isoToUTCTimestamp(c.ts),
      value: c.volume,
    }))
  }, [candles])

  const emaLine = useMemo(() => {
    const closes = candles.map(c => c.close)
    const e = ema(closes, emaPeriod)
    return candles.map((c, i) => ({
      time: isoToUTCTimestamp(c.ts),
      value: e[i],
    }))
  }, [candles, emaPeriod])

  useEffect(() => {
    if (!elRef.current) return

    const chart = createChart(elRef.current, {
      autoSize: true,
      layout: {
        background: { color: "rgba(0,0,0,0)" },
        textColor: "rgba(231,233,238,.9)",
        fontFamily: "ui-sans-serif, system-ui",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,.06)" },
        horzLines: { color: "rgba(255,255,255,.06)" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "rgba(110,231,255,.22)", style: LineStyle.Dotted },
        horzLine: { color: "rgba(110,231,255,.22)", style: LineStyle.Dotted },
      },
      timeScale: {
        borderColor: "rgba(255,255,255,.08)",
        rightOffset: 6,
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,.08)",
      },
    })

    const candleSeries = chart.addCandlestickSeries({
      upColor: "rgba(52,211,153,.95)",
      downColor: "rgba(251,113,133,.95)",
      borderVisible: false,
      wickUpColor: "rgba(52,211,153,.75)",
      wickDownColor: "rgba(251,113,133,.75)",
    })

    const emaSeries = chart.addLineSeries({
      color: "rgba(110,231,255,.9)",
      lineWidth: 2,
    })

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
      color: "rgba(255,255,255,.12)",
    })
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    })

    chartRef.current = chart
    candleSeriesRef.current = candleSeries
    emaSeriesRef.current = emaSeries
    volumeSeriesRef.current = volumeSeries

    const ro = new ResizeObserver(() => chart.timeScale().fitContent())
    ro.observe(elRef.current)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = null
      candleSeriesRef.current = null
      emaSeriesRef.current = null
      volumeSeriesRef.current = null
    }
  }, [])

  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current) return
    candleSeriesRef.current.setData(data)
    volumeSeriesRef.current.setData(vol as any)
    chartRef.current?.timeScale().fitContent()
  }, [data, vol])

  useEffect(() => {
    if (!emaSeriesRef.current) return
    if (!showEMA) {
      emaSeriesRef.current.setData([])
      return
    }
    emaSeriesRef.current.setData(emaLine)
  }, [emaLine, showEMA])

  return <div ref={elRef} style={{ width: "100%", height: "100%" }} />
}
