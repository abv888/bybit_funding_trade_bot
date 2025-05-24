module.exports = {
  apps: [
    {
      name: 'funding-trading-bot',
      script: 'funding_arbitrage_bot.py',
      interpreter: 'python3',
      cwd: './',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        NODE_ENV: 'production'
      },
      error_file: './logs/trading-bot-error.log',
      out_file: './logs/trading-bot-out.log',
      log_file: './logs/trading-bot-combined.log',
      time: true,
      merge_logs: true,
      restart_delay: 5000,
      max_restarts: 10,
      min_uptime: '10s'
    },
    {
      name: 'funding-telegram-bot',
      script: 'telegram_bot.py',
      interpreter: 'python3',
      cwd: './',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '200M',
      env: {
        NODE_ENV: 'production'
      },
      error_file: './logs/telegram-bot-error.log',
      out_file: './logs/telegram-bot-out.log',
      log_file: './logs/telegram-bot-combined.log',
      time: true,
      merge_logs: true,
      restart_delay: 3000,
      max_restarts: 10,
      min_uptime: '5s'
    }
  ]
};