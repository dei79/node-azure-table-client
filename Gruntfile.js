// Gruntfile.js
module.exports = function (grunt) {
    // load all grunt tasks matching the `grunt-*` pattern
    require('load-grunt-tasks')(grunt);

    grunt.initConfig({
        mochaTest: {
            test: {
                options: {
                    reporter: 'spec',
                    clearRequireCache: true
                },
                // src: ['test/**/*.js', 'test-network/**/*.js']
                src: ['test/**/*.js']
            }
        },

        mocha_istanbul: {
            coverage: {
                src: 'test'
            }
        },

        watch: {
            js: {
                options: {
                    spawn: true,
                    interrupt: true,
                    debounceDelay: 250
                },
                files: ['Gruntfile.js', 'lib/**/*.js', 'test/**/*.js'],
                tasks: ['mochaTest']
            }
        }
    });

    grunt.registerTask('default', []);
    grunt.registerTask('test', ['mochaTest']);
    grunt.registerTask('coverage', ['mocha_istanbul:coverage']);
};