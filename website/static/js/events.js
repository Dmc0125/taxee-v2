
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
    * @param {string} encoded
    */
function decodeHex(encoded) {
    let str = ""
    for (let i = 0; i < encoded.length; i += 2) {
        const hexByte = encoded.substring(i, i + 2)
        str += String.fromCharCode(parseInt(hexByte, 16))
    }
    return str
}

/**
    * @param {string} requestId
    * @param {(html: string, done: boolean) => void} updateFn
    */
function listenForUpdates(requestId, updateFn) {
    try {
        const requestConn = new EventSource(`/sync_request?id=${requestId}`)

        requestConn.addEventListener("update", function(e) {
            /** @type {string} */
            const d = e.data
            const [status, encodedHtml] = d.split(",")
            const btnComponentHtml = decodeHex(encodedHtml)
            updateFn(btnComponentHtml, status == "done")

            if (status == "done") {
                requestConn.close()
            }
        })

        requestConn.addEventListener("error", function(e) {
            console.error("request connection error: ", e.data)
            requestConn.close()
        })
    } catch {
        console.error("streaming is not supported")
    }
}


/**
    * @param {string} requestType
    * @returns {Promise<{ rid: string; html: string } | null>}
    */
async function handleClickParse(requestType) {
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
    * @param {Element} el
    * @param {string} html
    * @returns {Element}
    */
function updateEl(el, html) {
    el.outerHTML = html
    return document.querySelector(`#${el.id}`)
}

let btnRefetchEl = document.querySelector("#btn-refetch")
let btnParseTxsEl = document.querySelector("#btn-parse-txs")
assertDefined(btnParseTxsEl, "#btn-parse-txs not defined")
let btnParseEventsEl = document.querySelector("#btn-parse-events")
let progressIndicatorEl = document.querySelector("#progress-indicator")
let parsing = false

if (progressIndicatorEl.hasAttribute("data-request-id")) {
    parsing = true
    const rid = progressIndicatorEl.getAttribute("data-request-id")

    listenForUpdates(rid, function(html, done) {
        progressIndicatorEl = updateEl(progressIndicatorEl, html)
        if (done) {
            // TODO: set btn as clickable
            parsing = false
        }
    })
}

btnParseTxsEl.addEventListener("click", async function() {
    if (parsing) {
        return
    }

    parsing = true
    const result = await handleClickParse(1)
    if (result == null) {
        return
    }

    const { rid, html } = result
    progressIndicatorEl = updateEl(progressIndicatorEl, html)

    listenForUpdates(rid, function(html, done) {
        progressIndicatorEl = updateEl(progressIndicatorEl, html)
        if (done) {
            // TODO: set btn as clickable
            parsing = false
        }
    })
})
