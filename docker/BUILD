load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")
load("//docker:galley_docker.bzl", "galley_docker_build")

# Use "manual" target tag to skip rules in the wildcard expansion

pkg_tar(
    name = "galley_tar",
    extension = "tar.gz",
    files = [
        "//cmd/gals",
    ],
    mode = "0755",
    package_dir = "/usr/local/bin/",
    tags = ["manual"],
)

galley_docker_build(
    cmd = [
        "--logtostderr",
        "--v=2",
    ],
    entrypoint = [
        "/usr/local/bin/gals",
        "server",
    ],
    images = [
        {
            "name": "galley",
            "base": "@docker_ubuntu//:xenial",
        },
        {
            "name": "galley_debug",
            "base": "@ubuntu_xenial_debug//file",
        },
    ],
    ports = [
        "9096",
    ],
    repository = "istio",
    tags = ["manual"],
    tars = [
        ":galley_tar",
    ],
)
