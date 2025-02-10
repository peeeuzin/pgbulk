import { format } from "util";

export class Logger {
  private quiet: boolean;
  private moduleName: string;

  constructor(moduleName: string, quiet: boolean) {
    this.quiet = quiet;
    this.moduleName = moduleName;
  }

  info(message: string, ...args: string[]) {
    if (!this.quiet) console.log(format(this.base() + message, ...args));
  }

  private base() {
    return format("[%s] %s ", this.moduleName, new Date().toLocaleString());
  }
}
