# Test watch events being transmitted to other peers

When a write happens at one member it should trigger any watchers listening on other peers once they get synced to.

Exact semantics as yet undefined but it should at least send the latest value.

1. Run `1.sh`
2. Run `2.sh`
3. Watch with `watch1.sh`
4. Watch with `watch2.sh`
5. Test the put `put1.sh`

Both of the watch streams should get an event with `Put /a b`.
