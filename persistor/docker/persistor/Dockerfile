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

# file used for building Persistor

FROM golang:1.18-alpine3.16 as intermediate

# Set environment variables needed for Go runtime
ENV GO111MODULE=on \
CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
HOME=/app

RUN apk update && apk add git && apk add openssh --no-cache

WORKDIR $HOME

COPY ./persistor/go.mod ./persistor/go.sum ./
RUN go mod download

COPY ./persistor .
COPY LICENSE ./licenses/

# Build the application.
RUN go mod tidy && go mod vendor && go build -mod=vendor -o persistor ./cmd/persistor/


# This is the image that will be built; we'll copy what
# was pulled by calling the vendor module here.
FROM alpine:3.16

# copy executable, config and certificates
COPY --from=intermediate /app/persistor /app/persistor
COPY --from=intermediate /app/config /app/config
COPY --from=intermediate /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=intermediate /app/licenses /app/licenses

# change to a non-root user for security
RUN adduser --disabled-password -h $HOME user && \
    chown -R user /app && \
    chmod -R 777 /app
USER user

EXPOSE 2112

ENV PRODUCT_NAME PERSISTOR

WORKDIR /app
CMD ["./persistor"]
