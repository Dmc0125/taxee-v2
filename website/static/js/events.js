import { fetchTokensImages } from "./tokens.js"
import { decodeHex } from "./utils.js"

/**
* @param {any} val
* @param {string} msg
*/
function assertDefined(val, msg) {
    if (val == null) {
        throw Error(`value is null: ${msg}`)
    }
}

/**
* @param {string} requestType
* @returns {Promise<{ rid: string; html: string } | null>}
*/
async function sendSyncRequest(requestType) {
    try {
        const res = await fetch(
            `/sync_request?type=${requestType}`,
            { method: "POST" },
        )
        if (res.status != 200) {
            console.error(await res.text())
            return
        }

        const data = await res.text()
        const [requestId, html] = data.split(",")

        return {
            rid: requestId,
            html,
        }
    } catch (err) {
        // Internet connection ??
        console.error("unable to create sync request: ", err)
        return null
    }
}

/** 
* @param {string} requestId
* @returns {Promise<string | null>}
*/
function handleSync(requestId) {
    console.log("handle sync")
    /** @type {EventSource} */
    let requestConn
    try {
        requestConn = new EventSource(`/sync_request?id=${requestId}`)
    } catch {
        console.error("streaming is not supported")
        return
    }

    requestConn.addEventListener("update", function(e) {
        const btnComponentEl = document.querySelector("#progress-indicator")
        const html = decodeHex(e.data)
        btnComponentEl.outerHTML = html
    })

    const p = new Promise(async function(resolve) {
        requestConn.addEventListener("error", function(e) {
            requestConn.close()
            reject(e.data)
        })

        requestConn.addEventListener("close", async function() {
            requestConn.close()

            const tableRes = await fetch(`/events${window.location.search}&partial=true`)
            const tableHtml = await tableRes.text()
            document.querySelector("#events-table").innerHTML = tableHtml

            await fetchTokensImages()
            resolve()
        })
    })

    return p
}

let parsing = false

document.addEventListener("DOMContentLoaded", async function() {
    const progressIndicatorEl = document.querySelector("#progress-indicator")

    if (progressIndicatorEl.hasAttribute("data-request-id")) {
        parsing = true
        const rid = progressIndicatorEl.getAttribute("data-request-id")

        await handleSync(rid)
        parsing = false
    } else {
        fetchTokensImages()
    }
})

// let btnRefetchEl = document.querySelector("#btn-refetch")
let btnParseTxsEl = document.querySelector("#btn-parse-txs")
assertDefined(btnParseTxsEl, "#btn-parse-txs not defined")
// let btnParseEventsEl = document.querySelector("#btn-parse-events")

btnParseTxsEl.addEventListener("click", async function() {
    if (parsing) {
        return
    }

    parsing = true
    const result = await sendSyncRequest(1)
    if (result == null) {
        return
    }

    const { rid, html } = result
    document.querySelector("#progress-indicator").outerHTML = html

    const err = await handleSync(rid)
    if (err != null) {
        console.error(err)
    }
    parsing = false
})


////////////////
// sticky table nav
// window.addEventListener("resize", function() {
//
// })
//

////////////////
// Sticky header

/** @type {Element} */
let tableHeaderEl
/** @type {number} */
let tableHeaderOffsetFromTop

// TODO: Browser for some reason does not keep the scroll position on reload
// if the style was applied when clicking reload
function makeEventsTableHeaderFloat() {
    const offset = window.scrollY - tableHeaderOffsetFromTop
    if (offset > 0) {
        tableHeaderEl.setAttribute("style", `top: ${offset}px`)
    } else if (offset < 0) {
        tableHeaderEl.setAttribute("style", "top: 0px")
    }
}

document.addEventListener("DOMContentLoaded", function() {
    tableHeaderEl = document.querySelector("#table-header")
    const tableHeaderRect = tableHeaderEl.getBoundingClientRect()
    tableHeaderOffsetFromTop = (tableHeaderRect.top + window.scrollY) - 8

    makeEventsTableHeaderFloat()
})

document.addEventListener("scroll", function() {
    makeEventsTableHeaderFloat()
})

