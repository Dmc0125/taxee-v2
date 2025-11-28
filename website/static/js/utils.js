/**
* @param {string} encoded
*/
export function decodeHex(encoded) {
    let str = ""
    for (let i = 0; i < encoded.length; i += 2) {
        const hexByte = encoded.substring(i, i + 2)
        str += String.fromCharCode(parseInt(hexByte, 16))
    }
    return str
}

