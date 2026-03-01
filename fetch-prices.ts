// fetch-prices/index.ts
// פונקציה זו מושכת מחירים מ-Yahoo Finance ומעדכנת את ה-DB
// להריץ: אוטומטי כל שעה, או ידני מפאנל האדמין

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

// ======== הגדרות ========
const YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
const DELAY_MS = 1200 // המתנה בין בקשות (כדי לא להיחסם)

// ממיר שם בורסה לסיומת Yahoo
function toYahooSymbol(ticker: string, exchange: string): string {
  if (exchange === "TASE") return `${ticker}.TA`
  return ticker
}

// שולף מחיר בודד מ-Yahoo
async function fetchPrice(ticker: string, exchange: string) {
  const symbol = toYahooSymbol(ticker, exchange)
  const url = `${YAHOO_BASE}/${symbol}?interval=1d&range=5d`
  
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0" }
  })
  
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${symbol}`)
  
  const data = await res.json()
  const meta = data?.chart?.result?.[0]?.meta
  
  if (!meta) throw new Error(`No data for ${symbol}`)
  
  return {
    ticker,
    exchange,
    regular_price: meta.regularMarketPrice,
    pre_price: meta.preMarketPrice ?? null,
    after_price: meta.postMarketPrice ?? null,
    currency: meta.currency,
    market_state: meta.marketState, // REGULAR / PRE / POST / CLOSED
    timestamp: new Date(meta.regularMarketTime * 1000).toISOString()
  }
}

serve(async (req) => {
  // אפשר לקרוא גם מ-GET וגם מ-POST
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  )

  const results: any[] = []
  const errors: any[] = []

  try {
    // 1. שלוף את כל הטיקרים הייחודיים מהגשות
    const { data: items, error: itemsErr } = await supabase
      .from("submission_items")
      .select("ticker, exchange")
    
    if (itemsErr) throw itemsErr
    
    // הסר כפילויות
    const unique = [
      ...new Map(
        (items ?? []).map(i => [`${i.ticker}:${i.exchange}`, i])
      ).values()
    ]

    console.log(`Fetching prices for ${unique.length} symbols...`)

    // 2. שלוף מחיר לכל טיקר
    for (const item of unique) {
      await new Promise(r => setTimeout(r, DELAY_MS)) // המתן בין בקשות
      
      try {
        const priceData = await fetchPrice(item.ticker, item.exchange)
        
        // שמור ב-DB
        await supabase.from("market_prices").upsert({
          ticker: priceData.ticker,
          exchange: priceData.exchange,
          price: priceData.regular_price,
          pre_price: priceData.pre_price,
          after_price: priceData.after_price,
          session_type: "regular",
          price_timestamp: priceData.timestamp,
          fetched_at: new Date().toISOString()
        })

        results.push({ ticker: item.ticker, price: priceData.regular_price, ok: true })
        console.log(`✓ ${item.ticker}: ${priceData.regular_price}`)
        
      } catch (err) {
        console.error(`✗ ${item.ticker}:`, err.message)
        errors.push({ ticker: item.ticker, error: err.message })
      }
    }

    // 3. חשב תשואות לכל המשתתפים
    await recomputeAllReturns(supabase)

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
      JSON.stringify({ success: false, error: err.message }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    )
  }
})

// ======== חישוב תשואות ========
async function recomputeAllReturns(supabase: any) {
  // שלוף כל ההגשות עם הפריטים שלהן
  const { data: submissions } = await supabase
    .from("submissions")
    .select("id, submission_items(*)")
  
  for (const sub of submissions ?? []) {
    let totalReturn = 0
    let allValid = true

    for (const item of sub.submission_items ?? []) {
      // שלוף מחיר התחלה רשמי
      const { data: official } = await supabase
        .from("official_prices")
        .select("start_open")
        .eq("ticker", item.ticker)
        .eq("exchange", item.exchange)
        .maybeSingle()
      
      // שלוף מחיר נוכחי
      const { data: latest } = await supabase
        .from("market_prices")
        .select("price")
        .eq("ticker", item.ticker)
        .eq("exchange", item.exchange)
        .eq("session_type", "regular")
        .order("price_timestamp", { ascending: false })
        .limit(1)
        .maybeSingle()

      if (!official?.start_open || !latest?.price) {
        allValid = false
        continue
      }

      // asset_return = (current - start) / start * 100
      const assetReturn = (latest.price - official.start_open) / official.start_open * 100
      const contribution = (item.weight / 100) * assetReturn
      totalReturn += contribution

      // עדכן פריט
      await supabase.from("submission_items")
        .update({
          current_price: latest.price,
          asset_return: parseFloat(assetReturn.toFixed(4)),
          weighted_contribution: parseFloat(contribution.toFixed(4))
        })
        .eq("id", item.id)
    }

    if (allValid) {
      await supabase.from("submissions")
        .update({ 
          portfolio_return: parseFloat(totalReturn.toFixed(4)),
          last_computed_at: new Date().toISOString()
        })
        .eq("id", sub.id)
    }
  }
}
