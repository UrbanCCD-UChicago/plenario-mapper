/**
 * serves as a wrapper to all tests, exiting once all tests have completed
 * this way, you don't have to clean up all open resources - servers, sockets, etc.
 * this allows travis to continue with the build - otherwise the process will run forever and it will timeout
 */

// loads default reporter, but any other can be used
var reporter = require('nodeunit').reporters.default;
// safer exit, but process.exit(0) will do the same in most cases
var exit = require('exit');

reporter.run(['tests/travis_tests/unit_tests.js'], null, function(failures) {
    if (failures) {
        exit(1);
    }
    reporter.run(['tests/travis_tests/integration_tests.js'], null, function(failures) {
        if (failures) {
            exit(1);
        }
        exit(0);
    });
});
