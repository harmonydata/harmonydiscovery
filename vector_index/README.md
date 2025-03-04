# Vector index for Harmony Discovery

The Docker compose file defines the Weaviate index. The Weaviate index does not invoke HuggingFace - the API which is calling Weaviate should handle HuggingFace.

## Where is it running?

The vector index is running on an Azure VM with 16 GB RAM at IP `20.39.218.72`.

The Weaviate index is defined as a single Docker compose file at [docker-compose.yml](docker-compose.yml).

At present it's hosted on Azure but in future we can move to a different hosting provider or use Weaviate Serverless.

The Azure VM runs on an IP address and its domain/subdomain is defined by two A records in the DNS:

![arecords](docs/arecords.png)

## SSL Certificate Management

Since Azure VM by default does not have SSL certificates, we have added these manually

### How Certificates are Issued

Let's Encrypt Integration: Traefik is configured to obtain SSL certificates from Let's Encrypt using the ACME protocol.

Automatic Provisioning: The certresolver named myresolver is responsible for requesting and renewing certificates.

Domain Configuration: Certificates are issued for the domains defined in the traefik.http.routers labels of the Weaviate service (e.g., fastdatascience.com and grpc.fastdatascience.com).

Email Registration: The email specified in --certificatesresolvers.myresolver.acme.email is used for Let's Encrypt notifications and recovery.

Storage: Certificates are stored in ./letsencrypt/acme.json.

## SSL Renewal Process

Automatic Renewal:

Let's Encrypt issues certificates with a 90-day validity.

Traefik automatically renews them before expiry.

Renewal occurs in the background without requiring manual intervention.

## Validation Methods:

TLS Challenge (tlschallenge): Used for domain verification.

HTTP Challenge (httpchallenge): Requires the web entry point.

Manual Renewal (If Needed)

If the certificates do not renew automatically, you can manually force a renewal:

## Restart Traefik to trigger renewal

```
docker-compose restart traefik
```

If renewal issues persist, check the logs for errors:

```
docker logs traefik
```

## Troubleshooting

Ensure the domain names in Traefik labels match the actual domains.

Verify that ports 80 and 443 are accessible.

Check that Let's Encrypt has not rate-limited the domain due to excessive requests.

Inspect the acme.json file to confirm certificate storage.

## Notes

Modify the email in `--certificatesresolvers.myresolver.acme.email` to your own.

Use the Let's Encrypt staging environment for testing to avoid hitting rate limits.

Always restart Traefik if configuration changes are made
