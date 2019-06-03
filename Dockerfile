# ------------------------------------------------------------------------
from mcluseau/golang-builder:1.12.5 as build

# ------------------------------------------------------------------------
from alpine:3.9
entrypoint ["/bin/kafka2sql"]
copy --from=build /go/bin/ /bin/
