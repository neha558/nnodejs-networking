import { DataSourceOptions } from 'typeorm';

require('dotenv').config();

class ConfigService {
  constructor(private env: { [k: string]: string | undefined }) {}

  private getValue(key: string, throwOnMissing = true): string {
    const value = this.env[key];
    if (!value && throwOnMissing) {
      throw new Error(`config error - missing env.${key}`);
    }

    return value;
  }

  public ensureValues(keys: string[]) {
    keys.forEach((k) => this.getValue(k, true));
    return this;
  }

  public getPort() {
    return this.getValue('PORT', true);
  }

  public isProduction() {
    const mode = this.getValue('MODE', false);
    return mode != 'DEV';
  }

  public getDomain() {
    const domain = this.getValue('DOMAIN_NAME', false);
    return domain;
  }

  public getJWTSecret() {
    return this.getValue('JWT_SECRET', true);
  }

  public getEncryptionKey() {
    return this.getValue('ENCRYPTION_KEY', true);
  }

  public getNFTDistributionContract() {
    return this.getValue('NFT_DISTRIBUTION_CONTRACT', true);
  }

  public getNFTDistributionContractBlock() {
    return this.getValue('NFT_DISTRIBUTION_CONTRACT_BLOCK', true);
  }

  public getAdminEmail() {
    return this.getValue('ADMIN_EMAIL', true);
  }

  public getDisableCron() {
    return this.getValue('DISABLE_CRON', false) === 'true';
  }

  public getTypeOrmConfig(): DataSourceOptions {
    return {
      type: 'mysql',
      host: this.getValue('DATABASE_HOST'),
      port: parseInt(this.getValue('DATABASE_PORT')),
      username: this.getValue('DATABASE_USER'),
      password: this.getValue('DATABASE_PASSWORD'),
      database: this.getValue('DATABASE_DATABASE'),

      migrationsTableName: 'module_networking_migration',

      ssl: false,

      entities: ['dist/**/*.entity{.ts,.js}'],
      migrations: ['dist/src/migration/*.js'],
      // cli: {
      //   migrationsDir: 'dist/src/migration',
      // },
    };
  }

  getWalletAdmin() {
    return {
      username: this.getValue('WALLET_ADMIN_USERNAME', true),
      password: this.getValue('WALLET_ADMIN_PASSWORD', true),
    };
  }

  getByPassEmailVerification() {
    return this.getValue('BY_PASS_EMAIL_VERIFICATION', true) === 'true';
  }

  getSMTPDetails() {
    return {
      user: this.getValue('SMTP_USER', true),
      password: this.getValue('SMTP_PASSWORD', true),
      host: this.getValue('SMTP_HOST', true),
      port: this.getValue('SMTP_PORT', true),
      encryption: this.getValue('SMTP_ENCRYPTION', true),
      from: this.getValue('SMTP_FROM', true),
      fromEmail: this.getValue('SMTP_FROM_EMAIL', true),
    };
  }

  public getWalletContractAddress() {
    return this.getValue('WALLET_CONTRACT_ADDRESS', true);
  }

  public getMoralisAPIUrl() {
    return this.getValue('MORALIS_API_URL', true);
  }

  public getMoralisAPIKey() {
    return this.getValue('MORALIS_API_KEY', true);
  }

  public getTokenContractAddress() {
    return this.getValue('TOKEN_CONTRACT_ADDRESS', true);
  }

  public getChain() {
    return this.getValue('CHAIN', true);
  }

  public getPackContractAddress() {
    return this.getValue('PACK_CONTRACT_ADDRESS', true);
  }

  public getUSDTContractAddress() {
    return this.getValue('USDT_CONTRACT_ADDRESS', true);
  }

  public getDepositorPrivateKey() {
    return this.getValue('DEPOSITOR_PRIVATE_KEY', true);
  }

  public getDepositorAddress() {
    return this.getValue('DEPOSITOR_ADDRESS', true);
  }

  public getDecimals() {
    return this.getValue('DECIMALS', true);
  }

  public getMoralisSecret() {
    return this.getValue('MORALIS_SECRET', true);
  }

  public getUSDTWalletContract() {
    return this.getValue('USDT_WALLET_REQUESTS_CONTRACT', true);
  }

  public getUSDTWalletAuthorizedAddress() {
    return this.getValue('WALLET_AUTHORIZED_ADDRESS', true);
  }
}

const configService = new ConfigService(process.env).ensureValues([
  'DATABASE_HOST',
  'DATABASE_PORT',
  'DATABASE_USER',
  'DATABASE_PASSWORD',
  'DATABASE_DATABASE',
  'JWT_SECRET',
  'DOMAIN_NAME',
]);

export { configService };
