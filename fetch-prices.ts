// fetch-prices/index.ts — גרסה v3
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
const DELAY_MS = 1200

function toYahooSymbol(ticker: string, exchange: string): string {
  if (exchange === "TASE") return `${ticker}.TA`
  return ticker
}

async function fetchPrice(ticker: string, exchange: string) {
  const symbol = toYahooSymbol(ticker, exchange)
  const url = `${YAHOO_BASE}/${symbol}?interval=1d&range=5d`
  
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0" }
  })
  
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${symbol}`)
  
  const data = await res.json()
  const result = data?.chart?.result?.[0]
  const meta = result?.meta
  
  if (!meta) throw new Error(`No data for ${symbol}`)

  // שלוף מחיר סגירה אמיתי מה-OHLCV
  const closes = result?.indicators?.quote?.[0]?.close ?? []
  const timestamps = result?.timestamp ?? []
  
  // מצא את מחיר הסגירה האחרון שאינו null
  let closePrice = meta.regularMarketPrice
  for (let i = closes.length - 1; i >= 0; i--) {
    if (closes[i] !== null && closes[i] !== undefined) {
      closePrice = closes[i]
      break
    }
  }

  const lastTimestamp = timestamps.length > 0 
    ? new Date(timestamps[timestamps.length - 1] * 1000).toISOString()
    : new Date(meta.regularMarketTime * 1000).toISOString()
  
  return {
    ticker,
    exchange,
    regular_price: closePrice,
    pre_price: meta.preMarketPrice ?? null,
    after_price: meta.postMarketPrice ?? null,
    timestamp: lastTimestamp
  }
}

serve(async (_req) => {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  )

  const results: any[] = []
  const errors: any[] = []
  const priceMap: Record<string, number> = {} // ticker:exchange -> price

  try {
    // שלוף טיקרים ייחודיים
    const { data: items } = await supabase
      .from("submission_items")
      .select("ticker, exchange")

    const safeItems = items ?? []
    
    if (safeItems.length === 0) {
      return new Response(
        JSON.stringify({ success: true, updated: 0, errors: 0, message: "No submissions yet" }),
        { headers: { "Content-Type": "application/json" } }
      )
    }

    // הסר כפילויות
    const unique = [
      ...new Map(safeItems.map(i => [`${i.ticker}:${i.exchange}`, i])).values()
    ]

    console.log(`Fetching ${unique.length} symbols...`)

    // שלוף מחירים מ-Yahoo
    for (const item of unique) {
      await new Promise(r => setTimeout(r, DELAY_MS))
      
      try {
        const priceData = await fetchPrice(item.ticker, item.exchange)
        const key = `${item.ticker}:${item.exchange}`
        priceMap[key] = priceData.regular_price
        
        // עדכן market_prices
        await supabase.from("market_prices").upsert({
          ticker: priceData.ticker,
          exchange: priceData.exchange,
          price: priceData.regular_price,
          pre_price: priceData.pre_price,
          after_price: priceData.after_price,
          session_type: "regular",
          price_timestamp: priceData.timestamp,
          fetched_at: new Date().toISOString()
        }, { onConflict: "ticker,exchange,session_type" })

        results.push({ ticker: item.ticker, price: priceData.regular_price })
        
      } catch (err) {
        errors.push({ ticker: item.ticker, error: String(err) })
      }
    }

    // שלוף מחירי פתיחה רשמיים
    const { data: officialPrices } = await supabase
      .from("official_prices")
      .select("ticker, exchange, start_open")

    const officialMap: Record<string, number> = {}
    for (const op of officialPrices ?? []) {
      officialMap[`${op.ticker}:${op.exchange}`] = parseFloat(op.start_open)
    }

    // חשב תשואות ישירות מה-priceMap
    const { data: submissions } = await supabase
      .from("submissions")
      .select("id, submission_items(id, ticker, exchange, weight)")

    for (const sub of submissions ?? []) {
      let totalReturn = 0
      let validCount = 0

      for (const item of sub.submission_items ?? []) {
        const key = `${item.ticker}:${item.exchange}`
        const currentPrice = priceMap[key]
        const startPrice = officialMap[key]

        if (!currentPrice || !startPrice || startPrice === 0) continue

        const assetReturn = (currentPrice - startPrice) / startPrice * 100
        const contribution = (parseFloat(item.weight) / 100) * assetReturn
        totalReturn += contribution
        validCount++

        await supabase.from("submission_items")
          .update({
            current_price: currentPrice,
            asset_return: parseFloat(assetReturn.toFixed(4)),
            weighted_contribution: parseFloat(contribution.toFixed(4))
          })
          .eq("id", item.id)
      }

      if (validCount > 0) {
        await supabase.from("submissions")
          .update({ 
            portfolio_return: parseFloat(totalReturn.toFixed(4)),
            last_computed_at: new Date().toISOString()
          })
          .eq("id", sub.id)
      }
    }

    return new Response(
      JSON.stringify({ 
        success: true, 
        updated: results.length, 
        errors: errors.length, 
        details: results, 
        failed: errors 
      }),
      { headers: { "Content-Type": "application/json" } }
    )

  } catch (err) {
    return new Response(
      JSON.stringify({ success: false, error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    )
  }
})
