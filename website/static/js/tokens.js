import { decodeHex } from "./utils.js"

/** @returns {Promise<void>} */
export function fetchTokensImages() {
    const missingTokensElements = document.querySelectorAll("[data-coingecko-id]")
    /** @type {[]string} */
    const uniqueTokens = []
    let queryString = ""

    for (const el of missingTokensElements) {
        const coingeckoId = el.getAttribute("data-coingecko-id")
        if (!uniqueTokens.includes(coingeckoId)) {
            uniqueTokens.push(coingeckoId)
            queryString += `,${coingeckoId}`
        }
    }

    if (uniqueTokens.length == 0) {
        return Promise.resolve()
    }

    const conn = new EventSource(`/tokens?tokens=${queryString.slice(1)}`)

    conn.addEventListener("update", function(e) {
        /** @type {string} */
        const d = e.data
        const [coingeckoId, htmlEncoded] = d.split(",")
        const html = decodeHex(htmlEncoded)

        for (const el of missingTokensElements) {
            if (el.getAttribute("data-coingecko-id") == coingeckoId) {
                el.outerHTML = html
            }
        }
    })

    conn.addEventListener("error", function(e) {
        console.error("unable to fetch img for token: ", e.data)
    })

    const p = new Promise(function(res) {
        conn.addEventListener("close", function() {
            conn.close()
            res()
        })
    })

    return p
}

