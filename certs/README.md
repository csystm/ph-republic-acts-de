# certs/

## gsgccr3evtlsca2025.pem

Intermediate CA certificate for the Supreme Court E-Library
(`elibrary.judiciary.gov.ph`).

### Why it's here

The E-Library server sends an incomplete certificate chain — the leaf's real
issuer (GlobalSign GCC R3 EV TLS CA 2025) is omitted. Windows CryptoAPI
silently AIA-fetches the missing intermediate; OpenSSL does not, which
breaks the Docker container. See `src/extract/scrape_elibrary.py::_make_session`.

### Provenance

- Source URL: `http://secure.globalsign.com/cacert/gsgccr3evtlsca2025.crt`
  (the `CA Issuers - URI` from the server's leaf certificate)
- GlobalSign serves this file as **DER** (binary), not PEM. The version
  committed here was converted locally.
- Downloaded: 2026-10-05

### To refresh (if the intermediate expires or the server's chain changes)

1. Extract the current AIA URI from the server's leaf:

   ```bash
   openssl s_client -connect elibrary.judiciary.gov.ph:443 \
     -servername elibrary.judiciary.gov.ph </dev/null 2>/dev/null \
     | awk '/-----BEGIN CERTIFICATE-----/{n++} n==1' \
     | openssl x509 -text -noout | grep -A3 "Authority Information Access"
   ```

2. Download the `CA Issuers - URI` (it will be `.crt` — that's DER):

   ```bash
   curl -o /tmp/intermediate.crt <URI-FROM-STEP-1>
   ```

3. Convert to PEM:

   ```bash
   openssl x509 -in /tmp/intermediate.crt -inform DER -outform PEM \
       -out certs/gsgccr3evtlsca2025.pem
   ```

4. Verify the chain before committing:

   ```bash
   openssl s_client -connect elibrary.judiciary.gov.ph:443 \
     -servername elibrary.judiciary.gov.ph </dev/null 2>/dev/null \
     | awk '/-----BEGIN CERTIFICATE-----/{n++} n==1' > /tmp/leaf.pem

   openssl verify \
     -CAfile "$(python -c 'import certifi; print(certifi.where())')" \
     -untrusted certs/gsgccr3evtlsca2025.pem \
     /tmp/leaf.pem
   # -> /tmp/leaf.pem: OK
   ```