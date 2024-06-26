# codecrafter-test

I am learning rust by https://codecrafters.io, and using this project to verify my progress.

This is a hobby project, and I am not affiliated with codecrafters.io.

## Usage

This project is almost wrote with unit tests by go, so you should install go first, [go.dev/doc/install](https://go.dev/doc/install)

Echo project as one test.go file, the order of the test is the same as the course unless codecrafters.io updates the course.

📢📢📢: When you fail on a case which you think your implementation is correct, feel free to skip my test or open a issue. Since this is not the official test tool, some judge conditions may not be different. It's better to pay [the referral](https://app.codecrafters.io/r/fantastic-monkey-146935).

### Build your own Redis

```
$ cd redis
YOUR_REDIS_PROGRAM_PATH=???/codecrafters-redis-rust/spawn_redis_server.sh go test -run "TestBindAPort" -v
```

## Supported Courses

| Course                     | Status          | Link                                             | My implementation                    |
| -------------------------- | --------------- | ------------------------------------------------ | ------------------------------------ |
| Build your own HTTP server | Free            | https://app.codecrafters.io/courses/http-server  | [➡️](./codecrafters-http-server-rust) |
| Build your own Redis       | ✅               | https://app.codecrafters.io/courses/redis/       | [➡️](./codecrafters-redis-rust)       |
| Build your own DNS server  | `dig` is enough | https://app.codecrafters.io/courses/dns-server// | [➡️](./codecrafters-dns-server-rust)  |

All of my implementations are very straightforward, mostly just pass the test cases. So the code design may not be good, but it's still readable.
Also, because of the test, so refactoring is not hard and fun, I really recommend you to use this project to learn rust by refactoring my implementation.

## Important

If this project infringes your business interests, please contact me to delete it.
