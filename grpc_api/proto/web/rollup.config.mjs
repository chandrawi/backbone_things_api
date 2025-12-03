import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import excludeDependenciesFromBundle from "rollup-plugin-exclude-dependencies-from-bundle";

export default {
    input: 'src/index.js',
    output: {
        file: 'index.js',
        format: 'es'
    },
    plugins: [
        excludeDependenciesFromBundle({
            dependencies: true
        }),
        resolve({
            browser: true
        }),
        commonjs()
    ]
};
