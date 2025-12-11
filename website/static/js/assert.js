/** 
* @template T
* @param {T | null | undefined} v 
* @param {string} [message=""] 
* @returns {asserts v is T}}
*/
export function notNull(v, message = "") {
    if (v == null || v == undefined) {
        let m = "notNull assertion failed"
        if (message) {
            m += ": " + message
        }
        throw Error(m)
    }
}

/**
    * @template {new (...args: any[]) => any} TClass
    * @param {unknown} v 
    * @param {TClass} c 
    * @param {string} [message=""] 
    * @returns {asserts v is InstanceType<TClass>}
    */
export function instanceOf(v, c, message = "") {
    if (!(v instanceof c)) {
        let m = "instanceOf assertion failed"
        if (message) {
            m += ": " + message
        }
        throw Error(m)
    }
}
