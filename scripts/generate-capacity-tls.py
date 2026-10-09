#!/usr/bin/env python3
"""Create a new private, short-lived three-node capacity TLS fixture using OpenSSL."""
import argparse
from pathlib import Path
import subprocess


def generate(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    extension = directory / "leaf.ext"
    extension.write_text("basicConstraints=critical,CA:FALSE\n"
                         "keyUsage=critical,digitalSignature\n"
                         "extendedKeyUsage=serverAuth,clientAuth\n"
                         "subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    with (directory / "generation.log").open("x") as log:
        def openssl(*arguments):
            subprocess.run(["openssl", *map(str, arguments)], stdout=log, stderr=subprocess.STDOUT, check=True)

        openssl("genpkey", "-algorithm", "ED25519", "-out", directory / "ca.key")
        openssl("req", "-new", "-x509", "-key", directory / "ca.key", "-out", directory / "ca.crt",
                "-subj", "/CN=Cellule capacity fixture CA", "-days", "1",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign,cRLSign,digitalSignature")
        for index in range(3):
            key = directory / f"node-{index}.key"
            csr = directory / f"node-{index}.csr"
            openssl("genpkey", "-algorithm", "ED25519", "-out", key)
            openssl("req", "-new", "-key", key, "-out", csr, "-subj", "/CN=localhost")
            openssl("x509", "-req", "-in", csr, "-CA", directory / "ca.crt", "-CAkey", directory / "ca.key",
                    "-CAcreateserial", "-out", directory / f"node-{index}.crt", "-days", "1", "-extfile", extension)
        for key in directory.glob("*.key"):
            key.chmod(0o600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new directory outside the source checkout")
    generate(parser.parse_args().directory.absolute())
