/** @type {import('next').NextConfig} */
const nextConfig = {
    async rewrites() {
        return [
            {
                source: '/api/:route',
                destination: (process.env.API_URL ?? 'http://127.0.0.1:8000') + '/:route',
            },
        ]
    },
    env: {
        api: process.env.API_URL ?? 'http://127.0.0.1:8000',
    },
    // Docker configuration
    output: "standalone",
}

module.exports = nextConfig
