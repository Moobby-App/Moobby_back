# Moobby Apple Push preparation

Status: source and migration PREPARED, not deployed. No migration, new DB login,
APNs connection, live push, or LaunchAgent activation is performed during tests.
Existing feed, NLP publication code, model choices and Redis remain independent.

## Backend design

1. Apply `migrations/019_apns_notification_outbox.sql` explicitly through an
   authorized administrator. New `notifications` tables and an event-version trigger
   atomically enqueue the first verified/published event. Rolling back publication
   rolls back its outbox insert. No historical event backfill runs.
2. Registration stores installation UUID, SHA-256 installation-secret digest,
   APNs token, station IDs, severity preferences, environment and topic.
3. `fan_out()` reads the committed app-safe projection. Station overlap and severity
   preferences select already-registered recipients; one event/device unique key
   prevents duplicate queue entries. New subscribers do not get historical alerts.
4. The sender leases one delivery at a time (`SKIP LOCKED`, claim UUID, 60s lease),
   releases the DB transaction, then sends HTTP/2 to APNs. A maximum of five attempts
   and bounded backoff apply to transient transport, 429 and server failures.
5. The server skips expired events and disabled installations. APNs expiration uses
   event expiry (light 30m, moderate 1h, severe 3h), not send time plus a new lifetime.
   Invalid tokens are disabled without revoking a newer token registration.

`accepted` records APNs HTTP 200, NOT proof that a device received/read the alert.
Transport delivery is at-least-once: an acceptance followed by a DB failure can
cause a retry. Stable `apns-id`, event collapse ID and app event-ID deduplication
reduce duplicates; they do not guarantee exactly-once banners. No automatic update
or recovery push is implemented: this version sends initial event notifications only.
Reports assigned different event IDs can still generate separate notifications even
if they describe the same real incident; semantic event deduplication is not added here.

This initial package supports ONE APNs environment/topic per database deployment.
Use separate development/production databases/deployments. The outbox fan-out marker
must not be shared across multiple topics/environments. Future multi-app support
needs an outbox target dimension. Pending jobs for disappeared/retracted events are
not sent; retention/cleanup scheduling is not enabled in this preparation package.

## Required backend setup

- Apple Developer membership, push-enabled App ID, exact bundle ID (`apns-topic`).
- An APNs signing key `.p8`, Key ID, Team ID, appropriate environment permission.
  Keep the private key outside the app and Git; `.p8`/`.p12` are ignored.
- Separate least-privilege DB login DSNs. An administrator assigns the registration
  login membership in `mooby_notifications_api_dev`, sender login membership in
  `mooby_notifications_sender_dev`. Do not reuse worker, admin or read-only feed DSNs.
- Apply migration 019; test the trigger and SQL permissions in Development before
  enabling production. SQL has only been locally inspected/tested with doubles.
- Install sender dependencies via `.venv/bin/python -m pip install '.[push]'` on
  the target runtime. The current local environment has the dependencies installed.

Private `.env` settings (placeholders are not usable values):

```dotenv
MOOBY_PUSH_REGISTRATION_ENABLED=1
MOOBY_PUSH_API_DATABASE_URL=postgresql://DEDICATED_REGISTRATION_LOGIN/...
MOOBY_APNS_ENABLED=1
MOOBY_APNS_ENVIRONMENT=sandbox
MOOBY_APNS_TOPIC=YOUR_APP_BUNDLE_ID
MOOBY_APNS_TEAM_ID=YOUR_TEAM_ID
MOOBY_APNS_KEY_ID=YOUR_KEY_ID
MOOBY_APNS_KEY_PATH=/absolute/private/path/AuthKey.p8
MOOBY_PUSH_SENDER_DATABASE_URL=postgresql://DEDICATED_SENDER_LOGIN/...
```

Default flags in `.env.example` are 0. The API supervisor forwards registration
settings only; signing key, Team ID and APNs enable flag never enter the API child.
The NLP worker does not send notifications or require APNs credentials.

After migration, DB roles, credentials and a sandbox device are configured:

```sh
launchctl kickstart -k "gui/$(id -u)/dev.mooby.api"
.venv/bin/python -B scripts/run_push_sender.py --env-file .env
```

The second command CAN SEND REAL PUSH when enabled. Do not run with production
credentials as a dry run. It processes up to 25 outbox events and 100 deliveries,
then exits. For continuous processing, the prepared, uninstalled agent
`ops/launchd/dev.mooby.push.plist` schedules it every 15 seconds. Only after a sandbox
send is verified, copy the agent to `~/Library/LaunchAgents/`, then explicitly load
it with `launchctl bootstrap`. No automatic agent installation is performed.

## iOS steps

1. In Xcode Signing & Capabilities enable **Push Notifications** for the correct
   App ID and provisioning profile. SwiftUI apps can bridge an AppDelegate using
   `@UIApplicationDelegateAdaptor`. Do not enable background modes solely to receive
   normal visible alert pushes; this implementation is not a silent background push.
2. Explain the purpose and ask consent with
   `UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge])`.
   Set a `UNUserNotificationCenterDelegate` early for foreground presentation and taps.
3. Register via `UIApplication.shared.registerForRemoteNotifications()` on the main
   actor. In `application(_:didRegisterForRemoteNotificationsWithDeviceToken:)`, hex
   encode every byte (`String(format: "%02x", byte)`) and register it with the API.
   APNs tokens are variable length; never assume exactly 64 hex characters. Obtain
   the current token from APNs each launch; do not persist a token as authoritative.
4. Create an installation UUID and a cryptographically random **32-byte secret**,
   base64url-encoded without padding (43 characters). Persist the UUID and secret
   together in Keychain. This is separate from APNs token and shared API bearer.
   Reuse them for token rotation/preferences updates. Never print any of these secrets.
5. Once permission is granted, token available and preferences chosen, send:

```http
PUT /api/v1/push/devices/<installation-uuid>
Authorization: Bearer <existing-api-bearer>
X-Installation-Secret: <43-character-base64url-secret>
Content-Type: application/json

{
  "device_token": "<current-token-as-hex>",
  "station_ids": ["open-krl:SRP", "open-krl:THB"],
  "severities": ["light", "moderate", "severe"]
}
```

The body does NOT choose environment or topic; backend config determines those.
Success: `{"installation_id":"...","status":"registered"}`. PUT again when token
or preferences change. Preferences replace the old arrays. There must be at least
one station and severity. Send DELETE to disable, rather than empty arrays.

6. On opt-out/logout or when notification permission is revoked, call:

```http
DELETE /api/v1/push/devices/<installation-uuid>
Authorization: Bearer <existing-api-bearer>
X-Installation-Secret: <same-secret>
```

Check permission on foreground return, because Settings changes have no guaranteed
in-app callback. Keep credentials long enough to unregister. Retry registration or
unregistration on transient network/503 errors; do not spin on 401/403/409/422.
403 means wrong installation secret, 409 token already bound to another installation.
Do not overwrite a different installation identity to bypass either response.
Keychain identity loss/token ownership recovery requires an authenticated recovery
design (e.g. account/App Attest), not automatic reassignment in this preparation.

7. Handle the custom payload:

```json
{
  "aps": {"alert":{"title":"...","body":"..."},"sound":"default","thread-id":"moobby-disruptions"},
  "schema_version": 1,
  "type": "disruption_published",
  "event_id": "<public-event-uuid>",
  "severity": "moderate",
  "expires_at": "<ISO-8601 UTC timestamp>"
}
```

On tap, open the disruption feed and refresh via the authenticated API, using the
event UUID to locate/highlight the relevant item when present. Do not invent a
detail endpoint: this preparation adds device endpoints, not a new event-detail API.
Handle missing/expired events and pagination gracefully. Never treat push text as
the authoritative current event state. Suppress duplicate/expired in-app alerts by
event ID/expiry. iOS may already have displayed an OS banner while your app was not
running; expiry cannot retroactively remove it. On foreground, query delivered
notifications and remove expired ones by their actual notification request IDs.
Always call notification delegate completion handlers.

8. Debug builds generally use sandbox; TestFlight/App Store builds use production.
   Match the signed `aps-environment` entitlement, token, key permissions, backend
   environment and topic. A sandbox token cannot be sent to the production host.
   Test denied permission, token rotation, opt-out, foreground/background/terminated
   app, expired event, duplicate delivery and deep-link navigation on a signed device.

## Limits before production

Existing bearer authentication is retained, but a shared app bearer is not a user
identity or abuse-control mechanism. Installation-secret ownership protects updates;
rate limiting, account ownership/App Attest, token-at-rest encryption and explicit
retention/deletion policy remain production hardening work. Dedicated DB credentials
and private schemas must not be shipped to iOS. Keep APNs private keys backend-only.
APNs acceptance/delivery is best effort; the feed API remains the source of truth.

Apple references:
- https://developer.apple.com/documentation/usernotifications/registering-your-app-with-apns
- https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns
- https://developer.apple.com/documentation/usernotifications/establishing-a-token-based-connection-to-apns
