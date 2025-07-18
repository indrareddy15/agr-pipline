/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                primary: '#1890ff',
                secondary: '#52c41a',
                accent: '#fa8c16',
                danger: '#ff4d4f',
                warning: '#faad14',
                success: '#52c41a',
                info: '#1890ff',
            },
        },
    },
    plugins: [],
    corePlugins: {
        preflight: false, // Disable Tailwind's base styles to avoid conflicts with Ant Design
    },
}