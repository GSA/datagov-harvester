// rollup.config.js
import copy from 'rollup-plugin-copy'

export default {
  input: "./rollup.js",
  plugins: [
    copy({
      targets: [
        { src: './node_modules/chart.js/dist/chart.umd.js', dest: './assets/chartjs/' },
        { src: './node_modules/chart.js/dist/chart.umd.js.map', dest: './assets/chartjs/' },
        { src: './node_modules/htmx.org/dist/htmx.min.js', dest: './assets/htmx/' },
      ]
    })
  ]
}
