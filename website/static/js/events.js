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
            return null
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
        const btnComponentEls = document.querySelectorAll("#progress-indicator")
        const html = decodeHex(e.data)
        for (const el of btnComponentEls) {
            el.outerHTML = html
        }
    })

    const p = new Promise(async function(resolve) {
        requestConn.addEventListener("error", function(e) {
            requestConn.close()
            reject(e.data)
        })

        requestConn.addEventListener("close", async function() {
            requestConn.close()

            let eventsUrl = "/events"
            const q = window.location.search
            if (q == "") {
                eventsUrl += "?partial=true"
            } else {
                eventsUrl += `${q}&partial=true`
            }

            const tableRes = await fetch(eventsUrl)
            const tableHtml = await tableRes.text()
            document.querySelector("#events-table").innerHTML = tableHtml

            await fetchTokensImages()
            resolve()
        })
    })

    return p
}

{
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
        const els = document.querySelectorAll("#progress-indicator")
        for (const el of els) {
            el.outerHTML = html
        }

        const err = await handleSync(rid)
        if (err != null) {
            console.error(err)
        }
        parsing = false
    })
}

////////////////
// sticky table nav
{
    const floatingTableNav = document.querySelector("#table-nav-floating")
    let applied = false

    window.addEventListener("scroll", function() {
        if (window.scrollY > 100 && !applied) {
            floatingTableNav.classList.remove("pointer-events-none", "opacity-0")
            applied = true
        } else if (applied && window.scrollY < 100) {
            floatingTableNav.classList.add("pointer-events-none", "opacity-0")
            applied = false
        }
    })
}

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

////////////////
// Event sources toggle

document.addEventListener("DOMContentLoaded", function() {
    const events = document.querySelectorAll("[data-event-with-sources]")
    /** @type {Map<string, boolean>} */
    const opened = new Map()

    for (const event of events) {
        const sourcesEl = event.nextElementSibling

        event.addEventListener("click", function() {
            const id = event.Id
            opened[id] = !opened[id]

            if (opened[id]) {
                sourcesEl.classList.remove("hidden")
            } else {
                sourcesEl.classList.add("hidden")
            }
        })
    }

})



