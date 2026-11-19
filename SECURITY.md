# Security policy

jgroups is a Rust port of the JGroups cluster transport: it reads protocol messages from the network, input its users
do not control, so a vulnerability in it matters. Please report one privately, and give
us the chance to fix it before it is public.

## Reporting a vulnerability

Report it through GitHub's private vulnerability reporting: on the repository's **Security** tab, choose
**Report a vulnerability**, or go straight to
[the form](https://github.com/oxidestack/jgroups/security/advisories/new). Only the maintainers see the report.

**Do not open a public issue, discussion or pull request for a vulnerability.**

What helps us most: the crate version or commit, the call and the input that trigger it (a message, a byte sequence), and what an attacker gets (a panic, unbounded memory or CPU, a forged or misrouted message).

## What happens next

We answer in the report's thread, confirm the problem or explain why it is not one, and agree with you on a fix and a
date to disclose it. The fix is released with a GitHub security advisory, a RustSec advisory where one applies, and an
entry in the release notes; you are credited in the advisory unless you would rather not be.

## Supported versions

Security fixes go into the latest release. Upgrade to it to get them; earlier releases are not patched.

## What is in scope

The crate's own code. A difference from JGroups's behaviour is a bug, to report as one, unless it also breaks a security
boundary. A vulnerability in JGroups itself belongs to [its maintainers](https://github.com/belaban/JGroups).
