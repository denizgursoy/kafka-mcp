# Authorization error

Work out which access control entry (ACL) is refusing a client, and what
permission it is missing.

Use this when the user says "payments gets TOPIC_AUTHORIZATION_FAILED", "the
consumer can't join its group", "GROUP_AUTHORIZATION_FAILED", "why can't this
service write to orders", or "what is this user allowed to do".

## Tools

| Tool            | Use it for                                                    |
| --------------- | ------------------------------------------------------------- |
| `server_config` | The identity **this server** connects as (`sasl_user`, `sasl_options`), which is not the client's |
| `list_acls`     | The ACLs for a principal, a resource, or both                 |

## Steps

### 1. Establish the principal and the refused operation

Two facts are needed, and the error message alone rarely has both:

- **The client's principal**, including its type: `User:payments`, not
  `payments`. With SASL/SCRAM it is the username; with mTLS it is usually the
  certificate's distinguished name. Ask the user rather than guessing.
- **What it was doing** when refused, which decides the operation it needed:

| The client was | Needs on the resource |
| -------------- | --------------------- |
| Producing | `write` on the topic (and `describe`) |
| Consuming | `read` on the topic **and** `read` on the group |
| Committing offsets | `read` on the group |
| Producing transactionally | `write` on the transactional id, plus the topic |
| Creating a topic | `create` on the topic or the cluster |
| Changing topic config | `alter_configs` on the topic |

`GROUP_AUTHORIZATION_FAILED` on a consumer that can read the topic means the
group ACL is missing, which is the most common half-done setup.

### 2. List what applies

Call `list_acls` with the principal, and separately with the resource:

```json
{"principal": "User:payments"}
{"resource_type": "topic", "resource_name": "orders"}
```

The resource query returns every ACL the broker applies to that name — literal,
prefixed (`pattern_type: prefixed`, whose `resource_name` is a prefix), and the
`*` wildcard — so it covers what actually decides access.

### 3. Read it the way the broker does

- **A `deny` wins.** One matching deny overrides every allow, whatever its
  pattern. Look for denies first.
- **No match means no access**, unless the cluster sets
  `allow.everyone.if.no.acl.found`. An empty list for the principal is the
  answer "it has no permissions at all".
- **Host matters.** An ACL with a host other than `*` applies only to clients
  connecting from that address.
- **Implied operations.** `read`, `write`, `delete` and `alter` imply
  `describe`; `alter_configs` implies `describe_configs`.

State the missing permission exactly: principal, operation, resource type,
resource name, pattern. That is what whoever administers the cluster needs.

### 4. Hand over, do not fix

This server does not create ACLs. Granting access is a decision for the
cluster's administrators, and the precise request from step 3 is what they need.

## Notes

- `SECURITY_DISABLED` means the broker has no authorizer, so ACLs are not
  enforced at all. Then an authorization error is not coming from ACLs; check
  whether a proxy or a managed service's own IAM layer is refusing the client.
- `server_config` reports the SASL identity **this server** uses, in
  `sasl_user` and `sasl_options`; with SASL/SCRAM or PLAIN the principal is
  `User:<sasl_user>`. An mTLS certificate identity is not reported, and neither
  is an OAuth token's subject; ask the operator for those. Listing ACLs needs
  `describe` on the cluster for that principal; a refusal there is about this
  server's rights, not the client's.
- `read_only` on an endpoint is this server's own policy and never shows up in
  ACLs; see the Permissions section of the README.
