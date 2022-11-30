/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./source/_templates/**/*.html"],
  theme: {
    extend: {
      colors: {
        "grey-40": "#9696A0",
        "grey-80": "#3A3A44",
      },
    },
    container: {
      center: true,
      padding: "1rem",
    },
  },
  plugins: [],
  corePlugins: {
    preflight: false, // We use preflight from Furo theme instead.
  },
};
