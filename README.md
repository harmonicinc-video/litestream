> **Note:** This is an internal Harmonic Inc. fork of the excellent [Litestream](https://github.com/benbjohnson/litestream) project originally created by Ben Johnson.

Litestream
![GitHub release (latest by date)](https://img.shields.io/github/v/release/harmonicinc-video/litestream)
![Status](https://img.shields.io/badge/status-beta-blue)
![GitHub](https://img.shields.io/github/license/harmonicinc-video/litestream)
![test](https://github.com/harmonicinc-video/litestream/workflows/test/badge.svg)
==========

Litestream is a standalone disaster recovery tool for SQLite. It runs as a
background process and safely replicates changes incrementally to another file
or S3. Litestream only communicates with SQLite through the SQLite API so it
will not corrupt your database.

If you need support or have ideas for improving Litestream, please visit the [GitHub Discussions](https://github.com/harmonicinc-video/litestream/discussions).

## Acknowledgements

Litestream was created by Ben Johnson. The project is incredibly grateful to the following people for their help and
contributions! Without them, this project would not be what it is today.

- [David Crawshaw](https://github.com/crawshaw) - Lots of early help brainstorming
  and writing the initial C implementations of the interceptor.
- [Michael Malis](https://github.com/mmalis) - Helping to work out some complex
  concurrency issues.
- [Kurtis Nusbaum](https://github.com/kurtisn) - Poring over logs &
  investigating bizarre synchronization bugs.
- [Alexey Kutelev](https://github.com/z0rr0) - Helping refactor & write tests.
- Plus many other amazing [contributors](https://github.com/benbjohnson/litestream/graphs/contributors) who have put their time and
  energy into the project to help make it better:

Huge thanks to fly.io for their support and for contributing credits for testing and development!

## Contribution Policy

Litestream is open to internal code contributions. Please submit a pull request or file an issue.

[new-issue]: https://github.com/harmonicinc-video/litestream/issues/new
