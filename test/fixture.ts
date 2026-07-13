import { ConfigSchema } from '../src/types/config.js';

export const baseConfig = ConfigSchema.parse({
  source: {
    dbUrl: 'postgresql://source:password@source.example.com/postgres',
    apiUrl: 'https://source.example.com',
    serviceRoleKey: 'header.payload.signature',
  },
  target: {
    dbUrl: 'postgresql://target:password@target.example.com/postgres',
    apiUrl: 'https://target.example.com',
    serviceRoleKey: 'header.payload.signature',
  },
  options: {},
});
