# @cumulus/discover-pdrs

[![Build Status](https://travis-ci.org/nasa/cumulus.svg?branch=master)](https://travis-ci.org/nasa/cumulus)

Discover PDRs in FTP/HTTP/HTTPS/SFTP/S3 endpoints
## Message Configuration
### Config

| field name | default | description
| --------   | ------- | ----------
| provider   | (required) | The cumulus-api provider object
| collection | (required) | The cumulus-api collection object
| bucket     | (required) | The internal bucket name (used for record keeping)
| stack      | (required) | Cumulus deployment stack name

### Input

| field name | default | description
| --------   | ------- | ----------
| N/A        | N/A     | N/A

## What is Cumulus?

Cumulus is a cloud-based data ingest, archive, distribution and management
prototype for NASA's future Earth science data streams.

[Cumulus Documentation](https://nasa.github.io/)

## Contributing

See [Cumulus README](https://github.com/nasa/cumulus/blob/master/README.md#installing-and-deploying)
