import re


HRP = "kmq"
ADDRESS_BYTE_LENGTH = 32
LEGACY_HEX_RE = re.compile(r"^[0-9a-fA-F]{64}$")
CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
CHARSET_MAP = {char: index for index, char in enumerate(CHARSET)}
BECH32M_CONST = 0x2BC830A3


class AddressCodecError(ValueError):
    pass


def encode_address(address_bytes):
    if len(address_bytes) != ADDRESS_BYTE_LENGTH:
        raise AddressCodecError(f"Address must be {ADDRESS_BYTE_LENGTH} bytes.")

    data = _convertbits(address_bytes, 8, 5, True)
    checksum = _create_checksum(HRP, data)
    return f"{HRP}1{''.join(CHARSET[value] for value in data + checksum)}"


def decode_address(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        raise AddressCodecError("Address is required.")

    if LEGACY_HEX_RE.fullmatch(raw_value):
        return bytes.fromhex(raw_value.lower())

    if raw_value.lower() != raw_value and raw_value.upper() != raw_value:
        raise AddressCodecError("Mixed-case addresses are invalid.")

    normalized = raw_value.lower()
    separator_index = normalized.rfind("1")
    if separator_index <= 0:
        raise AddressCodecError("Address is missing the separator.")

    hrp = normalized[:separator_index]
    if hrp != HRP:
        raise AddressCodecError(f"Address must start with '{HRP}1'.")

    payload = normalized[separator_index + 1 :]
    if len(payload) < 6:
        raise AddressCodecError("Address checksum is missing.")

    try:
        values = [CHARSET_MAP[character] for character in payload]
    except KeyError as exc:
        raise AddressCodecError(f"Address contains invalid character '{exc.args[0]}'.") from exc

    if not _verify_checksum(hrp, values):
        raise AddressCodecError("Address checksum is invalid.")

    decoded = bytes(_convertbits(values[:-6], 5, 8, False))
    if len(decoded) != ADDRESS_BYTE_LENGTH:
        raise AddressCodecError(f"Address must decode to {ADDRESS_BYTE_LENGTH} bytes.")

    return decoded


def normalize_address(value):
    return encode_address(decode_address(value))


def _polymod(values):
    generators = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    checksum = 1
    for value in values:
        top = checksum >> 25
        checksum = ((checksum & 0x1FFFFFF) << 5) ^ value
        for index in range(5):
            if (top >> index) & 1:
                checksum ^= generators[index]
    return checksum


def _hrp_expand(hrp):
    return [ord(char) >> 5 for char in hrp] + [0] + [ord(char) & 31 for char in hrp]


def _create_checksum(hrp, data):
    values = _hrp_expand(hrp) + data
    polymod = _polymod(values + [0, 0, 0, 0, 0, 0]) ^ BECH32M_CONST
    return [(polymod >> 5 * (5 - index)) & 31 for index in range(6)]


def _verify_checksum(hrp, data):
    return _polymod(_hrp_expand(hrp) + data) == BECH32M_CONST


def _convertbits(data, from_bits, to_bits, pad):
    accumulator = 0
    bits = 0
    result = []
    max_value = (1 << to_bits) - 1
    max_accumulator = (1 << (from_bits + to_bits - 1)) - 1

    for value in data:
        if value < 0 or value >> from_bits:
            raise AddressCodecError("Address contains invalid data.")
        accumulator = ((accumulator << from_bits) | value) & max_accumulator
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            result.append((accumulator >> bits) & max_value)

    if pad:
        if bits:
            result.append((accumulator << (to_bits - bits)) & max_value)
    elif bits >= from_bits or ((accumulator << (to_bits - bits)) & max_value):
        raise AddressCodecError("Address padding is invalid.")

    return result
