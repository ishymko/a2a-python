# Changelog

## 1.0.0-alpha.0 (2026-03-12)


### ⚠ BREAKING CHANGES

* **client:** introduce ServiceParameters for extensions and include it in ClientCallContext ([#784](https://github.com/ishymko/a2a-python/issues/784))
* **client:** rename "callback" -> "push_notification_config" ([#749](https://github.com/ishymko/a2a-python/issues/749))

### Features

* Add `protocol_version` column to Task and PushNotificationConfig models and create a migration ([#789](https://github.com/ishymko/a2a-python/issues/789)) ([2e2d431](https://github.com/ishymko/a2a-python/commit/2e2d43190930612495720c372dd2d9921c0311f9))
* add GetExtendedAgentCardRequest as input parameter to GetExtendedAgentCard method ([#767](https://github.com/ishymko/a2a-python/issues/767)) ([13a092f](https://github.com/ishymko/a2a-python/commit/13a092f5a5d7b2b2654c69a99dc09ed9d928ffe5))
* Add validation for the JSON-RPC version ([#808](https://github.com/ishymko/a2a-python/issues/808)) ([6eb7e41](https://github.com/ishymko/a2a-python/commit/6eb7e4155517be8ff0766c0a929fd7d7b4a52db5))
* **client:** expose close() and async context manager support on abstract Client ([#719](https://github.com/ishymko/a2a-python/issues/719)) ([e25ba7b](https://github.com/ishymko/a2a-python/commit/e25ba7be57fe28ab101a9726972f7c8620468a52))
* **compat:** AgentCard backward compatibility helpers and tests ([#760](https://github.com/ishymko/a2a-python/issues/760)) ([81f3494](https://github.com/ishymko/a2a-python/commit/81f349482fc748c93b073a9f2af715e7333b0dfb))
* **compat:** GRPC client compatible with 0.3 server ([#779](https://github.com/ishymko/a2a-python/issues/779)) ([0ebca93](https://github.com/ishymko/a2a-python/commit/0ebca93670703490df1e536d57b4cd83595d0e51))
* **compat:** GRPC Server compatible with 0.3 client ([#772](https://github.com/ishymko/a2a-python/issues/772)) ([80d827a](https://github.com/ishymko/a2a-python/commit/80d827ae4ebb6515bf8dcb10e50ba27be8b6b41b))
* **compat:** legacy v0.3 protocol models, conversion logic and utilities ([#754](https://github.com/ishymko/a2a-python/issues/754)) ([26835ad](https://github.com/ishymko/a2a-python/commit/26835ad3f6d256ff6b84858d690204da66854eb9))
* **compat:** REST and JSONRPC clients compatible with 0.3 servers. ([#798](https://github.com/ishymko/a2a-python/issues/798)) ([08794f7](https://github.com/ishymko/a2a-python/commit/08794f7bd05c223f8621d4b6924fc9a80d898a39)), closes [#742](https://github.com/ishymko/a2a-python/issues/742)
* **compat:** REST and JSONRPC servers compatible with 0.3 clients. ([#795](https://github.com/ishymko/a2a-python/issues/795)) ([9856054](https://github.com/ishymko/a2a-python/commit/9856054f8398162b01e38b65b2e090adb95f1e8b)), closes [#742](https://github.com/ishymko/a2a-python/issues/742)
* **compat:** set a2a-version header to 1.0.0 ([#764](https://github.com/ishymko/a2a-python/issues/764)) ([4cb68aa](https://github.com/ishymko/a2a-python/commit/4cb68aa26a80a1121055d11f067824610a035ee6))
* handle tenant in Client ([#758](https://github.com/ishymko/a2a-python/issues/758)) ([5b354e4](https://github.com/ishymko/a2a-python/commit/5b354e403a717c3c6bf47a291bef028c8c6a9d94))
* implement missing push notifications related methods ([#711](https://github.com/ishymko/a2a-python/issues/711)) ([041f0f5](https://github.com/ishymko/a2a-python/commit/041f0f53bcf5fc2e74545d653bfeeba8d2d85c79))
* **rest:** add tenant support to rest ([#773](https://github.com/ishymko/a2a-python/issues/773)) ([4771b5a](https://github.com/ishymko/a2a-python/commit/4771b5aa1dbae51fdb5f7ff4324136d4db31e76f))
* send task as a first subscribe event ([#716](https://github.com/ishymko/a2a-python/issues/716)) ([e71ac62](https://github.com/ishymko/a2a-python/commit/e71ac6266f506ec843d00409d606acb22fec5f78))
* **server, grpc:** Implement tenant context propagation for gRPC requests. ([#781](https://github.com/ishymko/a2a-python/issues/781)) ([164f919](https://github.com/ishymko/a2a-python/commit/164f9197f101e3db5c487c4dede45b8729475a8c))
* **server, json-rpc:** Implement tenant context propagation for JSON-RPC requests. ([#778](https://github.com/ishymko/a2a-python/issues/778)) ([72a330d](https://github.com/ishymko/a2a-python/commit/72a330d2c073ece51e093542c41ec171c667f312))
* **server:** add v0.3 legacy compatibility for database models ([#783](https://github.com/ishymko/a2a-python/issues/783)) ([08c491e](https://github.com/ishymko/a2a-python/commit/08c491eb6c732f7a872e562cd0fbde01df791cca))
* **server:** implement `Resource Scoping` for tasks and push notifications ([#709](https://github.com/ishymko/a2a-python/issues/709)) ([f0d4669](https://github.com/ishymko/a2a-python/commit/f0d4669224841657341e7f773b427e2128ab0ed8))
* use StreamResponse as push notifications payload ([#724](https://github.com/ishymko/a2a-python/issues/724)) ([a149a09](https://github.com/ishymko/a2a-python/commit/a149a0923c14480888c48156710413967dfebc36))


### Bug Fixes

* add history length and page size validations ([#726](https://github.com/ishymko/a2a-python/issues/726)) ([e67934b](https://github.com/ishymko/a2a-python/commit/e67934b06442569a993455753ee4a360ac89b69f)), closes [#515](https://github.com/ishymko/a2a-python/issues/515)
* **client:** align send_message signature with BaseClient ([#740](https://github.com/ishymko/a2a-python/issues/740)) ([57cb529](https://github.com/ishymko/a2a-python/commit/57cb52939ef9779eebd993a078cfffb854663e3e))
* handle parsing error in REST ([#806](https://github.com/ishymko/a2a-python/issues/806)) ([bbd09f2](https://github.com/ishymko/a2a-python/commit/bbd09f232f556c527096eea5629688e29abb3f2f))
* handle REST query params as per 1.0 spec ([#804](https://github.com/ishymko/a2a-python/issues/804)) ([45b3059](https://github.com/ishymko/a2a-python/commit/45b305989773546d75278eb29ae52d2c9be06951))
* incorporate latest 1.0 proto changes ([#788](https://github.com/ishymko/a2a-python/issues/788)) ([47a5959](https://github.com/ishymko/a2a-python/commit/47a5959b8648897b00b257472c1a45c63d92d403)), closes [#559](https://github.com/ishymko/a2a-python/issues/559)
* properly handle unset and zero history length ([#717](https://github.com/ishymko/a2a-python/issues/717)) ([72a1007](https://github.com/ishymko/a2a-python/commit/72a100797e513730dbeb80477c943b36cf79c957))
* remove v1 from HTTP+REST/JSON paths ([#765](https://github.com/ishymko/a2a-python/issues/765)) ([627ae0b](https://github.com/ishymko/a2a-python/commit/627ae0bc9e7da2b4dd133fc37c0ae194307d9f8b))
* use correct REST path for Get Extended Agent Card operation ([#769](https://github.com/ishymko/a2a-python/issues/769)) ([ced3f99](https://github.com/ishymko/a2a-python/commit/ced3f998a9d0b97495ebded705422459aa8d7398)), closes [#559](https://github.com/ishymko/a2a-python/issues/559)


### Code Refactoring

* **client:** introduce ServiceParameters for extensions and include it in ClientCallContext ([#784](https://github.com/ishymko/a2a-python/issues/784)) ([942f4ae](https://github.com/ishymko/a2a-python/commit/942f4ae714c1ae76ff11ce8654bf53a8760daa36))
* **client:** rename "callback" -&gt; "push_notification_config" ([#749](https://github.com/ishymko/a2a-python/issues/749)) ([7dec763](https://github.com/ishymko/a2a-python/commit/7dec763d68784b0f4196246ee4f1c64f4ac06c26))
