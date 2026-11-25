"""Beat 5: Client-side PyNaCl end-to-end encryption vs a breached server.

Run:
    python 03_e2e_encryption.py
"""

from __future__ import annotations

import sys

from nacl.exceptions import CryptoError
from nacl.public import Box, PrivateKey

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    print("=== Beat 5: End-to-End Encryption (PyNaCl Curve25519 + XSalsa20-Poly1305) ===\n")

    # Each phone generates its own Curve25519 keypair locally (private keys NEVER leave the device)
    alice_priv = PrivateKey.generate()
    alice_pub = alice_priv.public_key

    bob_priv = PrivateKey.generate()
    bob_pub = bob_priv.public_key

    secret_text = b"Meet at 9"

    # 1. Without E2EE: breached server reads plaintext straight off the wire
    print("[1] WITHOUT E2EE (Plaintext crossing breached server):")
    print(f"    Hacked server wire capture -> {secret_text.decode('utf-8')!r}  <-- PLAINTEXT EXPOSED!\n")

    # 2. With E2EE: encrypt on sender's phone BEFORE touching the network
    box = Box(alice_priv, bob_pub)
    cipher = box.encrypt(secret_text)

    print("[2] WITH E2EE (box = Box(my_priv, peer_pub); cipher = box.encrypt(text)):")
    print(f"    Original plaintext : {secret_text.decode('utf-8')!r}")
    print(f"    Hacked server sees : {cipher.hex()[:56]}... ({len(cipher)} bytes)  <-- 0% READABLE!\n")

    # 3. Tamper protection: if the hacked server flips 1 byte in transit, Poly1305 MAC rejects it
    tampered = bytearray(cipher)
    tampered[-1] ^= 0xFF
    bob_box = Box(bob_priv, alice_pub)

    print("[3] HACKED SERVER TAMPERS WITH 1 BYTE IN TRANSIT:")
    try:
        bob_box.decrypt(bytes(tampered))
        raise AssertionError("Tampered ciphertext should never decrypt!")
    except CryptoError as exc:
        print(f"    Bob's phone rejected forged packet -> CryptoError: {exc}\n")

    # 4. Untampered delivery: Bob decrypts cleanly with his private key + Alice's public key
    decrypted = bob_box.decrypt(cipher).decode("utf-8")
    print("[4] UNTAMPERED DELIVERY TO FRIEND'S PHONE:")
    print(f"    Bob decrypted -> {decrypted!r}  <-- SAFE & VERIFIED")


if __name__ == "__main__":
    main()
