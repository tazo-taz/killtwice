import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import { config } from './config';

@Injectable()
export class DbService implements OnModuleDestroy {
  // Small pools deliberately: the container has 512MB and the readers are
  // sequential, so a large pool buys nothing and costs memory.
  readonly source = new Pool({ connectionString: config.sourceDbUrl, max: 4 });
  readonly pipeline = new Pool({ connectionString: config.pipelineDbUrl, max: 6 });

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([this.source.end(), this.pipeline.end()]);
  }
}
