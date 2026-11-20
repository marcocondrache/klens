# Changelog

## [0.107.0](https://github.com/marcocondrache/klens/compare/v0.106.0...v0.107.0) (2026-11-19)


### Features

* **web:** add a columns menu to hide record table columns ([#744](https://github.com/marcocondrache/klens/issues/744)) ([c239c90](https://github.com/marcocondrache/klens/commit/c239c90e36851fc116e6287e9d59e81c6d687794))
* **web:** name the browser tab after the current view ([#758](https://github.com/marcocondrache/klens/issues/758)) ([8d25f2c](https://github.com/marcocondrache/klens/commit/8d25f2c2a5fc37e4c66d2f2e9eeb695f8c63a5e6))
* **web:** offer a retry when a table fails to load ([#757](https://github.com/marcocondrache/klens/issues/757)) ([e65a099](https://github.com/marcocondrache/klens/commit/e65a099dbabb9d1f88edf3b1d241d091a51fe428))


### Bug Fixes

* **deps:** bump hono and @hono/node-server to clear bun audit ([#745](https://github.com/marcocondrache/klens/issues/745)) ([7b9bfe6](https://github.com/marcocondrache/klens/commit/7b9bfe6faf0c123cc6e5a22a4bcddd159c26e288))
* **deps:** update dependency @tanstack/react-hotkeys to ^0.27.0 ([#736](https://github.com/marcocondrache/klens/issues/736)) ([2eab435](https://github.com/marcocondrache/klens/commit/2eab435ac4caee23366f4e5d7b7788788b43985b))
* **deps:** update dependency fumadocs-core to v16.19.3 ([#740](https://github.com/marcocondrache/klens/issues/740)) ([71cdf6f](https://github.com/marcocondrache/klens/commit/71cdf6fdfa76b7c0e352aca3fe70976c726ba23b))
* **deps:** update dependency fumadocs-ui to v16.19.3 ([#741](https://github.com/marcocondrache/klens/issues/741)) ([682aa98](https://github.com/marcocondrache/klens/commit/682aa98eb6397845319aabd895439b4f4a6b0b23))
* **web:** darken light-theme accent and status colors to pass text contrast ([#751](https://github.com/marcocondrache/klens/issues/751)) ([eb1bbc4](https://github.com/marcocondrache/klens/commit/eb1bbc4e6e844b8aa8e13ac1e9d4fa9a455acc09))
* **web:** draw focus rings in an opaque color that passes 3:1 ([#753](https://github.com/marcocondrache/klens/issues/753)) ([3dd9b92](https://github.com/marcocondrache/klens/commit/3dd9b9295d715d7602bc33c15a34c09b1df38b6e))
* **web:** drop overlay travel and scale under reduced motion ([#754](https://github.com/marcocondrache/klens/issues/754)) ([91f8871](https://github.com/marcocondrache/klens/commit/91f8871edfa834c0be66d0a8be822b5c0a10c4c8))
* **web:** pair status colors with an icon or label ([#755](https://github.com/marcocondrache/klens/issues/755)) ([7167673](https://github.com/marcocondrache/klens/commit/71676732945e3020c8992e70c6382123fc37a2ab))
* **web:** render meaningful muted values at full muted contrast ([#752](https://github.com/marcocondrache/klens/issues/752)) ([64e90fa](https://github.com/marcocondrache/klens/commit/64e90facc057d7e17e2a38e562b3498043fb8c8f))
* **web:** wrap page titles and record header keys instead of truncating ([#756](https://github.com/marcocondrache/klens/issues/756)) ([095a0c7](https://github.com/marcocondrache/klens/commit/095a0c7a29d5c361992057276383d02af7d3c718))

## [0.106.0](https://github.com/marcocondrache/klens/compare/v0.105.1...v0.106.0) (2026-11-12)


### Features

* **auth:** renew sessions with the provider's refresh token ([#729](https://github.com/marcocondrache/klens/issues/729)) ([d032e7c](https://github.com/marcocondrache/klens/commit/d032e7c49fb0c5a94870f8626ac7e8494b9b9e0f))


### Bug Fixes

* **deps:** update dependency @tanstack/react-hotkeys to ^0.23.0 ([#704](https://github.com/marcocondrache/klens/issues/704)) ([5d15423](https://github.com/marcocondrache/klens/commit/5d15423e30faa91878f44f482fea39bd5f236286))
* **deps:** update dependency fumadocs-core to v16.19.0 ([#730](https://github.com/marcocondrache/klens/issues/730)) ([a7cb838](https://github.com/marcocondrache/klens/commit/a7cb838e2a663d0db804362eb9b6cdd43297bb3b))
* **deps:** update dependency fumadocs-mdx to v15.6.0 ([#731](https://github.com/marcocondrache/klens/issues/731)) ([4f12a41](https://github.com/marcocondrache/klens/commit/4f12a41a3ad8385521ff39fce6ecaec173d5a130))
* **deps:** update dependency fumadocs-ui to v16.19.0 ([#732](https://github.com/marcocondrache/klens/issues/732)) ([bcc144c](https://github.com/marcocondrache/klens/commit/bcc144cb01f5ae813488f1e6d328fd07852c595a))
* **deps:** update rust crate krafka to 0.30.0 ([#733](https://github.com/marcocondrache/klens/issues/733)) ([1c2cda4](https://github.com/marcocondrache/klens/commit/1c2cda4023b3e76d5b8475f70d8a3d84bc5d2f0f))
* **deps:** update rust crate krafka to 0.31.0 ([#734](https://github.com/marcocondrache/klens/issues/734)) ([fcb3dce](https://github.com/marcocondrache/klens/commit/fcb3dce14fd560eafbdfc935532e526119a90dbd))
* **deps:** update rust crate schemreg to 0.7 ([#725](https://github.com/marcocondrache/klens/issues/725)) ([ca0518f](https://github.com/marcocondrache/klens/commit/ca0518f3219fe6e77d6424ad13d15a3a3011e15e))
* **kafka:** leave partitions without a committed offset out of group lag ([#726](https://github.com/marcocondrache/klens/issues/726)) ([a5e5258](https://github.com/marcocondrache/klens/commit/a5e525874e61f0402d6d90361b29ea55403db5a2))
