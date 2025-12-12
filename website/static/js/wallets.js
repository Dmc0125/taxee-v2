import * as assert from "./assert.js"

/**
* Replaces or inserts elements in the DOM based on attributes within the provided HTML.
* This function now supports processing multiple HTML fragments separated by `<!--delimiter-->`.
*
* For each HTML fragment:
* - If the fragment's root element has a `data-insert` attribute:
*   - `afterbegin`: The element will be inserted as the first child of the element whose ID
*     is specified in the `data-parent` attribute. The `data-parent` attribute is mandatory
*     for `afterbegin` insertions.
* - If the fragment's root element does not have a `data-insert` attribute:
*   - The function will attempt to replace the first existing element in the DOM
*     that matches the `id` attribute of the new element.
*
* @param {string} html A string containing one or more HTML fragments, potentially
*                      separated by `<!--delimiter-->`. Each fragment will be
*                      processed independently.
*/
function setElement(html) {
    const fragments = html.split("<!--delimiter-->")

    for (const fragment of fragments) {
        const _tempElement = document.createElement("div")
        _tempElement.innerHTML = fragment
        const newElement = _tempElement.firstElementChild
        assert.notNull(newElement)

        const insertPosition = newElement.getAttribute("data-insert")
        switch (insertPosition) {
            case "afterbegin": {
                const parentElementId = newElement.getAttribute("data-parent")
                assert.notNull(parentElementId, "missing parent element id for afterbegin insert")

                const parentElement = document.querySelector(`#${parentElementId}`)
                if (parentElement) {
                    parentElement.insertAdjacentHTML(insertPosition, fragment)
                }
                break
            }
            default: {
                const id = newElement.getAttribute("id")
                const deletedElement = document.querySelector(`#${id}`)
                if (deletedElement) {
                    deletedElement.outerHTML = fragment
                }
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
        console.log("unsubscribed from: ", url)
    })

    connection.addEventListener("error", function(/** @type {Event} */e) {
        console.error("Err: ", ("data" in e) && e.data)
        connection.close()
        connections.delete(url)
        console.log("unsubscribed from: ", url)
    })

    connection.addEventListener("update", function(/** @type {MessageEvent<string>} */ e) {
        setElement(e.data)
    })

    console.log("subscribed to: ", url)
}

/** @param {string} url */
function sseUnsubscribe(url) {
    const conn = connections.get(url)
    if (conn != null) {
        conn.close()
        connections.delete(url)
        console.log("unsubscribed from: ", url)
    }
}

/**
* Handles the submission of a form, preventing the default submission behavior.
* It sends the form data via a fetch request, and then processes the response.
*
* The function includes:
* - **Loading Indicator:** If the submitted button has a `data-spinner` attribute,
*   it will display a loading spinner (cloned from an element with `#loading-spinner` ID)
*   inside the button while the form is being submitted. The spinner's size can be
*   customized using the `data-spinner` value (e.g., "w-8" for width 8),
*   otherwise, it defaults to "w-5".
* - **Button Disabling:** The submitted button is disabled during the fetch request
*   to prevent multiple submissions.
*
* The function handles:
* - Unsubscribing from SSE if the form has a `data-sse-unsubscribe` attribute 
* - Deleting an element:
*   - If the form has a `data-delete-on-success` attribute with a value, the element
*     matching that ID will be removed from the DOM on successful submission.
*   - If the `data-delete-on-success` attribute is present but empty, the form itself
*     will be removed from the DOM on successful submission.
* - Updating the DOM with the response HTML using `setElement` on successful submission.
* - Subscribing to SSE if the response status is 202 (Accepted) and a 'Location' header is present.
*
* @param {SubmitEvent} e The submit event object.
*/
async function registerFormSubmit(e) {
    e.preventDefault()
    const t = e.target
    assert.instanceOf(t, HTMLFormElement)

    const btn = e.submitter
    if (!btn) {
        return
    }

    if (btn.getAttribute("disabled") == "true") {
        return
    }
    btn.setAttribute("disabled", "true")

    const loadingSpinnerElement = document.querySelector("#loading-spinner")
    assert.notNull(loadingSpinnerElement)

    /** @type {Element | null} */
    let spinnerElement = null
    const dataSpinner = btn.getAttribute("data-spinner")
    if (dataSpinner !== null) {
        btn.classList.add("relative")

        const tempElement = document.createElement("div")
        tempElement.innerHTML = loadingSpinnerElement.outerHTML

        assert.notNull(tempElement.firstElementChild)
        spinnerElement = tempElement.firstElementChild

        if (spinnerElement) {
            spinnerElement.classList.remove("hidden")

            if (dataSpinner.length > 0) {
                spinnerElement.firstElementChild?.classList.add(dataSpinner)
            } else {
                spinnerElement.firstElementChild?.classList.add("w-5")
            }
        }

        spinnerElement = btn.insertAdjacentElement(
            "beforeend", spinnerElement,
        )
    }

    const data = new FormData(t)

    const action = t.getAttribute("action")
    assert.notNull(action)
    const method = t.getAttribute("method")
    assert.notNull(method)

    // await new Promise((res) => setTimeout(() => res(null), 5000))

    const res = await fetch(action, {
        method: method,
        body: data,
    })

    if (!res.ok) {
        // TODO: what to do on error
        console.error("Err: ", res.status, await res.text())
    } else {
        //////////////////
        // handle sse unsubscribe
        const dataSseUnsubscribe = t.getAttribute("data-sse-unsubscribe")
        if (dataSseUnsubscribe !== null && dataSseUnsubscribe?.length > 0) {
            sseUnsubscribe(dataSseUnsubscribe)
        }

        //////////////////
        // update dom
        const dataDeleteOnSuccess = t.getAttribute("data-delete-on-success")
        if (dataDeleteOnSuccess != null) {
            if (dataDeleteOnSuccess.length > 0) {
                document.querySelector(`#${dataDeleteOnSuccess}`)?.remove()
            } else {
                t.remove()
            }
        }

        const html = await res.text()
        if (html.length > 0) {
            setElement(html)
        }

        //////////////////
        // handle sse subscribe
        if (res.status == 202) {
            const loc = res.headers.get("location")
            if (loc && loc.length > 0) {
                sseSubscribe(loc)
            }
        }
    }

    if (spinnerElement) {
        spinnerElement.remove()
    }
    btn.classList.remove("relative")
    btn.removeAttribute("disabled")
}

/**
 * Registers SSE subscriptions for all elements in the DOM that have a `data-sse-subscribe` attribute.
 * This function is intended to be called when the DOM is fully loaded.
 */
function registerSseSubscriptions() {
    const sseElements = document.querySelectorAll("[data-sse-subscribe]")

    for (let i = 0; i < sseElements.length; i += 1) {
        const el = sseElements[i]
        const sseUrl = el.getAttribute("data-sse-subscribe")
        if (sseUrl != null && sseUrl.length > 0) {
            sseSubscribe(sseUrl)
        }
    }
}

document.addEventListener("submit", registerFormSubmit)
document.addEventListener("DOMContentLoaded", registerSseSubscriptions)

