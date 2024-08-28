import fs = require('fs');
import { configService } from 'src/app/config/config.service';
fs.writeFileSync(
  'ormconfig.json',
  JSON.stringify(
    {
      ...configService.getTypeOrmConfig(),

      entities: ['**/*.entity{.ts,.js}'],

      migrationsTableName: 'networking_migration',

      migrations: ['src/migration/*.ts'],

      cli: {
        migrationsDir: 'src/migration',
      },
    },
    null,
    2,
  ),
);

console.log('DB config updated at ormconfig.json file...');
