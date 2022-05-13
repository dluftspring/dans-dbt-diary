{% macro create_f_convert_to_62() %}

create or replace function convert_to_62(STR string)
returns string
language javascript
AS $$
const DIGITS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const fromBase = 16
const toBase = 62

    const add = (x, y, base) => {
        let z = [];
        const n = Math.max(x.length, y.length);
        let carry = 0;
        let i = 0;
        while (i < n || carry) {
            const xi = i < x.length ? x[i] : 0;
            const yi = i < y.length ? y[i] : 0;
            const zi = carry + xi + yi;
            z.push(zi % base);
            carry = Math.floor(zi / base);
            i++;
        }
        return z;
    }

    const multiplyByNumber = (num, x, base) => {
        if (num < 0) return null;
        if (num == 0) return [];

        let result = [];
        let power = x;
        while (true) {
            num & 1 && (result = add(result, power, base));
            num = num >> 1;
            if (num === 0) break;
            power = add(power, power, base);
        }

        return result;
    }

    const parseToDigitsArray = (str, base) => {
        const digits = str.replace(/-/g,'').split('');
        let arr = [];
        for (let i = digits.length - 1; i >= 0; i--) {
            const n = DIGITS.indexOf(digits[i])
            if (n == -1) return null;
            arr.push(n);
        }
        return arr;
    }
    
    const formatUUID = (str, base) => {
        
        if (base == 16) {
            var uuid_str = str.padStart(32,'0')
            uuid_str = uuid_str.substring(0,8)
            + '-'
            + uuid_str.substring(8,12)
            + '-'
            + uuid_str.substring(12,16)
            + '-'
            + uuid_str.substring(16,20)
            + '-'
            + uuid_str.slice(20)
            
            return uuid_str
        }
        
        if (base == 62) {
            return str.padStart(22,'0')
        }
    }

    const digits = parseToDigitsArray(STR, fromBase);
    if (digits === null) return null;

    let outArray = [];
    let power = [1];
    for (let i = 0; i < digits.length; i++) {
        digits[i] && (outArray = add(outArray, multiplyByNumber(digits[i], power, toBase), toBase));
        power = multiplyByNumber(fromBase, power, toBase);
    }

    let out = '';
    for (let i = outArray.length - 1; i >= 0; i--)
        out += DIGITS[outArray[i]];
    
    return formatUUID(out, toBase);
$$


{% endmacro %}