import * as assert from "./assert.js"

/** 
* Replaces/inserts the element in DOM based on attributes of the element
* provided via the HTML
*
* if the element has `data-insert` attribute it will be inserted according
* to these rules:
* - `afterbegin` - insert as first child of element id in attribute `data-parent`
*
* if the element does not have `data-insert` attribute it will replace
* the first existing element that matches the attribute `id`
*
* @param {string} html 
*/
function setElement(html) {
    const _tempElement = document.createElement("div")
    _tempElement.innerHTML = html
    const newElement = _tempElement.firstElementChild
    assert.notNull(newElement)

    const insertPosition = newElement.getAttribute("data-insert")
    switch (insertPosition) {
        case "afterbegin": {
            const parentElementId = newElement.getAttribute("data-parent")
            assert.notNull(parentElementId, "missing parent element id for afterbegin insert")

            const parentElement = document.querySelector(`#${parentElementId}`)
            if (parentElement) {
                parentElement.insertAdjacentHTML(insertPosition, html)
            }
            return
        }
        default: {
            const id = newElement.getAttribute("id")
            const deletedElement = document.querySelector(`#${id}`)
            if (deletedElement) {
                deletedElement.outerHTML = html
            }
        }
    }
}

/** @type {Map<string, EventSource>} */
const connections = new Map()

/** @param {string} url */
function sseSubscribe(url) {
    if (connections.has(url)) {
        return
    }

    const connection = new EventSource(url)
    connections.set(url, connection)

    connection.addEventListener("close", function() {
        connection.close()
        connections.delete(url)
    })

    connection.addEventListener("error", function(/** @type {Event} */e) {
        console.error("Err: ", ("data" in e) && e.data)
        connection.close()
        connections.delete(url)
    })

    connection.addEventListener("update", function(/** @type {MessageEvent<string>} */ e) {
        setElement(e.data)
    })
}

document.addEventListener("submit", async function(e) {
    e.preventDefault()
    const t = e.target
    assert.instanceOf(t, HTMLFormElement)

    const data = new FormData(t)

    const action = t.getAttribute("action")
    assert.notNull(action)
    const method = t.getAttribute("method")
    assert.notNull(method)

    const res = await fetch(action, {
        method: method,
        body: data,
    })

    if (!res.ok) {
        console.error("Err: ", res.status, await res.text())
        return
    }

    if (res.status == 202) {
        const loc = res.headers.get("location")
        if (loc && loc.length > 0) {
            sseSubscribe(loc)
        }
    }

    const deleteElementId = t.getAttribute("data-delete")
    if (deleteElementId != null) {
        document.querySelector(`#${deleteElementId}`)?.remove()
    }

    const html = await res.text()
    if (html.length > 0) {
        setElement(html)
    }
})

document.addEventListener("DOMContentLoaded", function() {
    const sseElements = document.querySelectorAll("[data-sse]")

    for (let i = 0; i < sseElements.length; i += 1) {
        const el = sseElements[i]
        const sseUrl = el.getAttribute("data-sse")

        if (sseUrl != null && sseUrl.length > 0) {
            sseSubscribe(sseUrl)
        }
    }
})

