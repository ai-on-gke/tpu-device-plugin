# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM google-go.pkg.dev/golang:1.26.5@sha256:5075e8b0e1a7913c2274737c1a8b379ea264322c082bd21d3911d196dce46559 as builder
WORKDIR /go/src/tpu-device-plugin
COPY . .

RUN CGO_ENABLED=0 go build cmd/tpu/tpu.go
RUN chmod a+x /go/src/tpu-device-plugin/tpu

FROM gcr.io/distroless/base-debian13@sha256:57c1e4c72feb5925c4763ae4f6bd2013ad3854f57eff5b60dd9acb1ce0abc66e
COPY --from=builder /go/src/tpu-device-plugin/tpu /usr/bin/tpu-device-plugin
CMD ["/usr/bin/tpu-device-plugin", "-logtostderr"]
