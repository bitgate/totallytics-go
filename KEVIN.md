# KEVIN.md: totallytics-go

Go SDK for Totallytics API analytics, port of bitgate/totallytics-js (its WIRE.md v1 is the contract). Must stay PUBLIC (proxy.golang.org / pkg.go.dev).

## Layout
- Root module `github.com/bitgate/totallytics-go`, pkg `totallytics`, zero deps, `go 1.22`.
  - totallytics.go: Client, Options, record/flush/seal, measure, clip (UTF-16 aware like JS). aggregator.go, histogram.go (bucket + jsLog), transport.go (batch, retries, 413 split, pending cap 20), middleware.go (Start/Finish/Handler/Middleware, responseWriter), route.go + pattern_go122.go/pattern_go123.go.
- Adapters, own go.mod each with `replace => ../` (ignored by consumers): chi/ (pkg totallyticschi, chi v5.2.5), gin/ (totallyticsgin, gin v1.10.1), echo/ (totallyticsecho, echo/v4 v4.13.3). Floors = newest releases still on go <= 1.22. CI stable leg also tests framework @latest.
- testdata/conformance.json: `node testdata/gen-conformance.mjs ../totallytics-js > testdata/conformance.json` (needs the JS repo with dist/ built, node 22).
- CI: .github/workflows/ci.yml, matrix go 1.22 + stable x {root, chi, gin, echo}, GOTOOLCHAIN=local, vet, test -race, staticcheck (stable).

## Release
- Bump `Version` in version.go. Tag the same commit `vX.Y.Z`, `chi/vX.Y.Z`, `gin/vX.Y.Z`, `echo/vX.Y.Z`. If adapters need new root API, bump their `require github.com/bitgate/totallytics-go` first.
- Then fetch `https://proxy.golang.org/github.com/bitgate/totallytics-go/@v/vX.Y.Z.info` (and adapters) so pkg.go.dev indexes it.
- Push with the fine-grained PAT: the bitgate org rejects classic PATs (git and API, incl. the visibility PATCH).

## Decisions / gotchas
- Min Go 1.22 (ServeMux patterns). `Request.Pattern` is 1.23+ and set in place on the request the mux gets, so an outer Middleware sees it unless a layer in between copies the request (then wrap twice; nested Start only reports the route out). 1.22 build asks `mux.Handler(r)` when next is the *ServeMux itself. Subtree patterns ("/static/"), redirects, 404/405 -> raw path.
- bucket() uses jsLog = fdlibm log = Math.log in Node <= 26 (V8 < 2026-05-11). math.Log is off by 1 ulp on ~0.1% of inputs, which flips buckets at exact 1.08^k boundaries. The float64() conversions block FMA fusion (checked: no FMADD on arm64/ppc64le/s390x/riscv64/loong64/amd64v3). V8 >= 2026-05-11 uses LLVM libc (correctly rounded) log, so JS on newest V8 can differ by 1 bucket within a few ulps of a boundary. No practical effect.
- Deviations from JS: buffer rotates at MaxBatchRows (JS timer path waits for 10k keys); 499 for aborted or unwritten responses; 401 warning via Logger.Warn once per process, everything else Debug.
- chi: 404 inside Mount/Route records the mount pattern ("/admin/*"), top-level 404 the raw path. echo: 404 gives c.Path() "" -> raw path; adapter calls c.Error(err) then returns err (like echo's RequestLogger with HandleError).
- echo/v5 needs Go 1.25: separate adapter module if ever wanted.
- Dependabot shows ~40 alerts: all in the gin/echo go.mod minimums (x/crypto, x/net, x/text, x/sys, echo v4.13.3 GO-2026-6293). govulncheck symbol scan: 0 reachable. Every fixed version needs go >= 1.25 (x/crypto v0.56.0 needs 1.26), so a clean tab means dropping Go 1.22 for the adapters. Consumers get max(versions) via MVS, so the floors never downgrade anyone. Decision pending with Bart.

## Status
- v0.1.0, chi/v0.1.0, gin/v0.1.0, echo/v0.1.0 tagged at fbcf106 (CI #2 green), resolvable via proxy.golang.org on go1.22 and go1.27.

## Verified for v0.1.0
- Conformance vs totallytics-js v0.1.0 on node 22: 10307 bucket values (incl. +-2 ulp around every boundary), 6 batch scenarios, identical.
- Backend (read-only clone of bitgate/totallytics, local-only vitest file with keys mocked): 5 SDK batches from real traffic, 3804 metric + 180 error rows, normalizeBatch rejected 0, handleIngest 202 x5.
- Prod smoke with `tt_` + 48 zeros: 401 "invalid or revoked API key", SDK warned once and dropped.
