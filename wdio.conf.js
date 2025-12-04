export const config = {
  specs: ['./test/specs/**/*.js'],
  exclude: [],

  maxInstances: 1,

  capabilities: [{
    maxInstances: 1,
    'tauri:options': {
      application: './src-tauri/target/debug/holon-tauri'
    }
  }],

  logLevel: 'info',
  bail: 0,
  baseUrl: '',
  waitforTimeout: 10000,
  connectionRetryTimeout: 120000,
  connectionRetryCount: 3,

  services: [],

  framework: 'mocha',
  reporters: ['spec'],

  mochaOpts: {
    ui: 'bdd',
    timeout: 60000
  },

  port: 4445,
};
