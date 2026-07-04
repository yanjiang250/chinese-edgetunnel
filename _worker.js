import { connect } from 'cloudflare:sockets';

let userID = '你的UUID';
let proxyIP = '你的PROXYIP';
if (!是否有效用户识别码(userID)) {
    throw new Error('uuid is not valid');
}
export default {
    async fetch(request, env, ctx) {
        try {
            userID = env.UUID || userID;
            proxyIP = env.PROXYIP || proxyIP;
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                return new Response("Hello World!", { status: 200 });
            } else {
                return await 处理微累死通过网套请求(request);
            }
        } catch (err) {
            let e = err;
            return new Response(e.toString());
        }
    },
};

async function 处理微累死通过网套请求(request) {
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);
    webSocket.accept();
    let 地址 = '';
    let 端口随机日志 = '';
    const 记录日志 = (信息, 事件) => {
        console.log(`[${地址}:${端口随机日志}] ${信息}`, 事件 || '');
    };
    const 早期数据头部 = request.headers.get('sec-websocket-protocol') || '';
    const 可读网套流 = 创建可读网套流(webSocket, 早期数据头部, 记录日志);
    let 远程套接字包装器 = {
        value: null,
    };
    let 用户数据报写入 = null;
    let 是否域名系统 = false;

    可读网套流.pipeTo(new WritableStream({
        async write(数据块, 控制器) {
            if (是否域名系统 && 用户数据报写入) {
                return 用户数据报写入(数据块);
            }
            if (远程套接字包装器.value) {
                const 写入器 = 远程套接字包装器.value.writable.getWriter()
                await 写入器.write(数据块);
                写入器.releaseLock();
                return;
            }
            const {
                是否有错误,
                消息,
                远程端口 = 443,
                远程地址 = '',
                原始数据索引,
                微累死版本 = new Uint8Array([0, 0]),
                是否用户数据报,
            } = 处理微累死头部(数据块, userID);
            地址 = 远程地址;
            端口随机日志 = `${远程端口}--${Math.random()} ${是否用户数据报 ? 'udp ' : 'tcp ' } `;
            if (是否有错误) {
                throw new Error(消息);
                return;
            }
            if (是否用户数据报) {
                if (远程端口 === 53) {
                    是否域名系统 = true;
                } else {
                    throw new Error('用户数据报仅允许用于域名系统端口五十三');
                    return;
                }
            }
            const 微累死响应头部 = new Uint8Array([微累死版本[0], 0]);
            const 原始客户端数据 = 数据块.slice(原始数据索引);
            if (是否域名系统) {
                const { write } = await 处理用户数据报出站(webSocket, 微累死响应头部, 记录日志);
                用户数据报写入 = write;
                用户数据报写入(原始客户端数据);
                return;
            }
            处理传输控制协议出站(远程套接字包装器, 远程地址, 远程端口, 原始客户端数据, webSocket, 微累死响应头部, 记录日志);
        },
        close() {
            记录日志(`可读网套流已关闭`);
        },
        abort(原因) {
            记录日志(`可读网套流被中止`, JSON.stringify(原因));
        },
    })).catch((错误) => {
        记录日志('可读网套流管道错误', 错误);
    });
    return new Response(null, {
        status: 101,
        webSocket: client,
    });
}

async function 处理传输控制协议出站(远程套接字, 远程地址, 远程端口, 原始客户端数据, webSocket, 微累死响应头部, 记录日志) {
    async function 连接并写入(地址, 端口) {
        const tcpSocket = connect({
            hostname: 地址,
            port: 端口,
        });
        远程套接字.value = tcpSocket;
        记录日志(`已连接到 ${地址}:${端口}`);
        const 写入器 = tcpSocket.writable.getWriter();
        await 写入器.write(原始客户端数据);
        写入器.releaseLock();
        return tcpSocket;
    }
    async function 重试连接() {
        const tcpSocket = await 连接并写入(proxyIP || 远程地址, 远程端口)
        tcpSocket.closed.catch(错误 => {
            console.log('重试传输控制协议套接字关闭错误', 错误);
        }).finally(() => {
            安全关闭网套(webSocket);
        })
        远程套接字转网套(tcpSocket, webSocket, 微累死响应头部, null, 记录日志);
    }
    const tcpSocket = await 连接并写入(远程地址, 远程端口);
    远程套接字转网套(tcpSocket, webSocket, 微累死响应头部, 重试连接, 记录日志);
}

function 创建可读网套流(网套服务器, 早期数据头部, 记录日志) {
    let 可读流已取消 = false;
    const 流 = new ReadableStream({
        start(控制器) {
            网套服务器.addEventListener('message', (事件) => {
                if (可读流已取消) {
                    return;
                }
                const 消息 = 事件.data;
                控制器.enqueue(消息);
            });
            网套服务器.addEventListener('close', () => {
                安全关闭网套(网套服务器);
                if (可读流已取消) {
                    return;
                }
                控制器.close();
            });
            网套服务器.addEventListener('error', (错误) => {
                记录日志('网套服务器发生错误');
                控制器.error(错误);
            });
            const { earlyData, error } = 基础六十四转数组缓冲(早期数据头部);
            if (error) {
                控制器.error(error);
            } else if (earlyData) {
                控制器.enqueue(earlyData);
            }
        },
        pull(控制器) {
        },
        cancel(原因) {
            if (可读流已取消) {
                return;
            }
            记录日志(`可读流因${原因}被取消`)
            可读流已取消 = true;
            安全关闭网套(网套服务器);
        }
    });
    return 流;
}

function 处理微累死头部(微累死缓冲, 用户识别码) {
    if (微累死缓冲.byteLength < 24) {
        return {
            是否有错误: true,
            消息: '无效数据',
        };
    }
    const 版本 = new Uint8Array(微累死缓冲.slice(0, 1));
    let 是否有效用户 = false;
    let 是否用户数据报 = false;
    if (字符串化(new Uint8Array(微累死缓冲.slice(1, 17))) === 用户识别码) {
        是否有效用户 = true;
    }
    if (!是否有效用户) {
        return {
            是否有错误: true,
            消息: '无效用户',
        };
    }
    const 选项长度 = new Uint8Array(微累死缓冲.slice(17, 18))[0];
    const 命令 = new Uint8Array(
        微累死缓冲.slice(18 + 选项长度, 18 + 选项长度 + 1)
    )[0];
    if (命令 === 1) {
    } else if (命令 === 2) {
        是否用户数据报 = true;
    } else {
        return {
            是否有错误: true,
            消息: `命令 ${命令} 不支持`,
        };
    }
    const 端口索引 = 18 + 选项长度 + 1;
    const 端口缓冲 = 微累死缓冲.slice(端口索引, 端口索引 + 2);
    const 远程端口 = new DataView(端口缓冲).getUint16(0);
    let 地址索引 = 端口索引 + 2;
    const 地址缓冲 = new Uint8Array(
        微累死缓冲.slice(地址索引, 地址索引 + 1)
    );
    const 地址类型 = 地址缓冲[0];
    let 地址长度 = 0;
    let 地址值索引 = 地址索引 + 1;
    let 地址值 = '';
    switch (地址类型) {
        case 1:
            地址长度 = 4;
            地址值 = new Uint8Array(
                微累死缓冲.slice(地址值索引, 地址值索引 + 地址长度)
            ).join('.');
            break;
        case 2:
            地址长度 = new Uint8Array(
                微累死缓冲.slice(地址值索引, 地址值索引 + 1)
            )[0];
            地址值索引 += 1;
            地址值 = new TextDecoder().decode(
                微累死缓冲.slice(地址值索引, 地址值索引 + 地址长度)
            );
            break;
        case 3:
            地址长度 = 16;
            const 数据视图 = new DataView(
                微累死缓冲.slice(地址值索引, 地址值索引 + 地址长度)
            );
            const 网际协议第六版 = [];
            for (let i = 0; i < 8; i++) {
                网际协议第六版.push(数据视图.getUint16(i * 2).toString(16));
            }
            地址值 = 网际协议第六版.join(':');
            break;
        default:
            return {
                是否有错误: true,
                消息: `无效地址类型 ${地址类型}`,
            };
    }
    if (!地址值) {
        return {
            是否有错误: true,
            消息: `地址值为空，地址类型为 ${地址类型}`,
        };
    }
    return {
        是否有错误: false,
        远程地址: 地址值,
        地址类型,
        远程端口,
        原始数据索引: 地址值索引 + 地址长度,
        微累死版本: 版本,
        是否用户数据报,
    };
}

async function 远程套接字转网套(远程套接字, webSocket, 微累死响应头部, 重试, 记录日志) {
    let 远程数据块计数 = 0;
    let 微累死头部 = 微累死响应头部;
    let 有传入数据 = false;
    await 远程套接字.readable
    .pipeTo(
        new WritableStream({
            async write(数据块, 控制器) {
                有传入数据 = true;
                if (webSocket.readyState !== 网套就绪打开状态) {
                    控制器.error('网套未处于打开状态');
                }
                if (微累死头部) {
                    webSocket.send(await new Blob([微累死头部, 数据块]).arrayBuffer());
                    微累死头部 = null;
                } else {
                    webSocket.send(数据块);
                }
            },
            close() {
            },
            abort(原因) {
                console.error(`远程连接可读流中止`, 原因);
            },
        })
    )
    .catch((错误) => {
        console.error(`远程套接字转网套异常`, 错误);
        安全关闭网套(webSocket);
    });
    if (有传入数据 === false && 重试) {
        记录日志(`重试`)
        重试();
    }
}

function 基础六十四转数组缓冲(基础六十四字符串) {
    if (!基础六十四字符串) {
        return { error: null };
    }
    try {
        基础六十四字符串 = 基础六十四字符串.replace(/-/g, '+').replace(/_/g, '/');
        const 解码 = atob(基础六十四字符串);
        const 数组缓冲 = Uint8Array.from(解码, (字符) => 字符.charCodeAt(0));
        return { earlyData: 数组缓冲.buffer, error: null };
    } catch (错误) {
        return { error };
    }
}

function 是否有效用户识别码(用户识别码) {
    const 正则 = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return 正则.test(用户识别码);
}
const 网套就绪打开状态 = 1;
const 网套就绪关闭中状态 = 2;

function 安全关闭网套(套接字) {
    try {
        if (套接字.readyState === 网套就绪打开状态 || 套接字.readyState === 网套就绪关闭中状态) {
            套接字.close();
        }
    } catch (错误) {
        console.error('安全关闭网套错误', 错误);
    }
}

const 字节转十六进制 = [];
for (let i = 0; i < 256; ++i) {
    字节转十六进制.push((i + 256).toString(16).slice(1));
}
function 不安全字符串化(数组, 偏移 = 0) {
    return (字节转十六进制[数组[偏移 + 0]] + 字节转十六进制[数组[偏移 + 1]] + 字节转十六进制[数组[偏移 + 2]] + 字节转十六进制[数组[偏移 + 3]] + "-" + 字节转十六进制[数组[偏移 + 4]] + 字节转十六进制[数组[偏移 + 5]] + "-" + 字节转十六进制[数组[偏移 + 6]] + 字节转十六进制[数组[偏移 + 7]] + "-" + 字节转十六进制[数组[偏移 + 8]] + 字节转十六进制[数组[偏移 + 9]] + "-" + 字节转十六进制[数组[偏移 + 10]] + 字节转十六进制[数组[偏移 + 11]] + 字节转十六进制[数组[偏移 + 12]] + 字节转十六进制[数组[偏移 + 13]] + 字节转十六进制[数组[偏移 + 14]] + 字节转十六进制[数组[偏移 + 15]]).toLowerCase();
}
function 字符串化(数组, 偏移 = 0) {
    const 用户识别码 = 不安全字符串化(数组, 偏移);
    if (!是否有效用户识别码(用户识别码)) {
        throw TypeError("字符串化用户识别码无效");
    }
    return 用户识别码;
}

async function 处理用户数据报出站(webSocket, 微累死响应头部, 记录日志) {
    let 是否已发送微累死头部 = false;
    const 转换流 = new TransformStream({
        transform(数据块, 控制器) {
            for (let 索引 = 0; 索引 < 数据块.byteLength;) {
                const 长度缓冲 = 数据块.slice(索引, 索引 + 2);
                const 用户数据报长度 = new DataView(长度缓冲).getUint16(0);
                const 用户数据报数据 = new Uint8Array(
                    数据块.slice(索引 + 2, 索引 + 2 + 用户数据报长度)
                );
                索引 = 索引 + 2 + 用户数据报长度;
                控制器.enqueue(用户数据报数据);
            }
        },
    });
    转换流.readable.pipeTo(new WritableStream({
        async write(数据块) {
            const 响应 = await fetch('https://1.1.1.1/dns-query', {
                method: 'POST',
                headers: {
                    'content-type': 'application/dns-message',
                },
                body: 数据块,
            })
            const 域名系统查询结果 = await 响应.arrayBuffer();
            const 用户数据报大小 = 域名系统查询结果.byteLength;
            const 用户数据报大小缓冲 = new Uint8Array([(用户数据报大小 >> 8) & 0xff, 用户数据报大小 & 0xff]);
            if (webSocket.readyState === 网套就绪打开状态) {
                记录日志(`域名系统查询成功，消息长度为 ${用户数据报大小}`);
                if (是否已发送微累死头部) {
                    webSocket.send(await new Blob([用户数据报大小缓冲, 域名系统查询结果]).arrayBuffer());
                } else {
                    webSocket.send(await new Blob([微累死响应头部, 用户数据报大小缓冲, 域名系统查询结果]).arrayBuffer());
                    是否已发送微累死头部 = true;
                }
            }
        }
    })).catch((错误) => {
        记录日志('域名系统用户数据报出错' + 错误)
    });
    const 写入器 = 转换流.writable.getWriter();
    return {
        write(数据块) {
            写入器.write(数据块);
        }
    };
}