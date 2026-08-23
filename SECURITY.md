# Security Policy

## Reporting A Vulnerability

Please report suspected vulnerabilities privately before opening a public issue.
Email taewoong.kim@gmail.com with:

- the affected component or command;
- the version, commit, or image tag tested;
- a concise reproduction path;
- impact and any known workaround.

We will acknowledge receipt, coordinate remediation, and publish public details
after a fix or mitigation is available.

Do not include secrets, production credentials, or sensitive customer data in
the initial report. If encrypted transfer is needed, ask for a secure exchange
method in the first email.

## Supported Versions

| Version | Security updates |
| --- | --- |
| `main` | Yes |
| `v1.0.x` | Yes |
| Earlier or untagged releases | No |

## Supported Surface

Security reports are accepted for the public source tree, public build targets,
published container images, Kubernetes CSI manifests, and documented operator
APIs. Advanced Enterprise work, private validation tooling, and unpublished
release artifacts are handled through separate support channels.
