# Copyright 2024 Syntio Ltd.
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

FROM golang:1.18-alpine as build

ENV GO116MODULE=on \
    CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

RUN apk update && apk add --no-cache

WORKDIR /src

COPY ./resubmitter-api/go.mod ./resubmitter-api/go.sum ./
RUN go mod download

COPY ./resubmitter-api/ .
COPY LICENSE ./licenses/

RUN go mod tidy && \
    go build -ldflags "-w -s" -o /app/resubmitter ./cmd/resubmitter

FROM alpine

COPY --from=build /app/resubmitter /app/resubmitter
COPY --from=build /src/licenses /app/licenses

RUN adduser -D -h /app user && \
    chown -R user /app && \
    chmod -R 700 /app

USER user

EXPOSE 8080
ENTRYPOINT ["/app/resubmitter"]
