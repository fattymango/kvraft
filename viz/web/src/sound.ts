// Small WebAudio-only sound layer: short oscillator blips/booms/chimes,
// no audio files. Off by default per the spec.
export class Sound {
  private ctx: AudioContext | null = null;
  enabled = false;

  private ensureCtx(): AudioContext {
    if (!this.ctx) this.ctx = new AudioContext();
    return this.ctx;
  }

  private tone(freq: number, durationMs: number, type: OscillatorType = "sine", gain = 0.05) {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    const osc = ctx.createOscillator();
    const g = ctx.createGain();
    osc.type = type;
    osc.frequency.value = freq;
    g.gain.value = gain;
    osc.connect(g).connect(ctx.destination);
    const now = ctx.currentTime;
    g.gain.setValueAtTime(gain, now);
    g.gain.exponentialRampToValueAtTime(0.0001, now + durationMs / 1000);
    osc.start(now);
    osc.stop(now + durationMs / 1000);
  }

  blip() {
    this.tone(1200, 40, "square", 0.02);
  }

  boom() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    const osc = ctx.createOscillator();
    const g = ctx.createGain();
    osc.type = "sawtooth";
    osc.frequency.setValueAtTime(140, ctx.currentTime);
    osc.frequency.exponentialRampToValueAtTime(30, ctx.currentTime + 0.3);
    g.gain.setValueAtTime(0.15, ctx.currentTime);
    g.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.35);
    osc.connect(g).connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + 0.35);
  }

  chime() {
    this.tone(880, 150, "sine", 0.04);
  }

  // filtered white noise burst: the base for whooshes, cracks and rumbles
  private noise(durationMs: number, filter: BiquadFilterType, from: number, to: number, gain: number, delayMs = 0) {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    const t0 = ctx.currentTime + delayMs / 1000;
    const dur = durationMs / 1000;
    const buf = ctx.createBuffer(1, Math.ceil(ctx.sampleRate * dur), ctx.sampleRate);
    const data = buf.getChannelData(0);
    for (let i = 0; i < data.length; i++) data[i] = Math.random() * 2 - 1;
    const src = ctx.createBufferSource();
    src.buffer = buf;
    const f = ctx.createBiquadFilter();
    f.type = filter;
    f.frequency.setValueAtTime(from, t0);
    f.frequency.exponentialRampToValueAtTime(to, t0 + dur);
    const g = ctx.createGain();
    g.gain.setValueAtTime(gain, t0);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
    src.connect(f).connect(g).connect(ctx.destination);
    src.start(t0);
    src.stop(t0 + dur);
  }

  private sweep(type: OscillatorType, from: number, to: number, durationMs: number, gain: number, delayMs = 0) {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    const t0 = ctx.currentTime + delayMs / 1000;
    const dur = durationMs / 1000;
    const osc = ctx.createOscillator();
    const g = ctx.createGain();
    osc.type = type;
    osc.frequency.setValueAtTime(from, t0);
    osc.frequency.exponentialRampToValueAtTime(to, t0 + dur);
    g.gain.setValueAtTime(gain, t0);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
    osc.connect(g).connect(ctx.destination);
    osc.start(t0);
    osc.stop(t0 + dur);
  }

  /** falling-bomb whistle while the meteor drops */
  meteorFall(durationMs: number) {
    this.sweep("sine", 1400, 300, durationMs, 0.05);
    this.noise(durationMs, "bandpass", 3000, 600, 0.06);
  }

  /** deep thud + debris crunch on impact */
  meteorImpact() {
    this.sweep("sine", 120, 28, 700, 0.35);
    this.sweep("sawtooth", 90, 25, 400, 0.12);
    this.noise(900, "lowpass", 2500, 120, 0.4);
  }

  /** crystalline crackle as the ice block forms */
  freeze() {
    this.noise(450, "highpass", 3000, 9000, 0.12);
    for (let i = 0; i < 6; i++) this.tone(2400 + i * 380, 60, "triangle", 0.025);
    this.sweep("sine", 1800, 3600, 300, 0.03, 80);
  }

  /** glassy shatter when the ice breaks or melts */
  shatter() {
    this.noise(350, "highpass", 6000, 2000, 0.18);
    [3100, 4200, 2600, 5000].forEach((f, i) => this.sweep("triangle", f, f * 0.7, 160, 0.03, i * 45));
  }

  /** warbling sci-fi brain zap for amnesia */
  amnesia() {
    if (!this.enabled) return;
    const ctx = this.ensureCtx();
    const t0 = ctx.currentTime;
    const osc = ctx.createOscillator();
    const lfo = ctx.createOscillator();
    const lfoGain = ctx.createGain();
    const g = ctx.createGain();
    osc.type = "sawtooth";
    osc.frequency.setValueAtTime(600, t0);
    osc.frequency.exponentialRampToValueAtTime(90, t0 + 1.0);
    lfo.frequency.value = 18;
    lfoGain.gain.value = 120;
    lfo.connect(lfoGain).connect(osc.frequency);
    g.gain.setValueAtTime(0.0001, t0);
    g.gain.exponentialRampToValueAtTime(0.06, t0 + 0.08);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + 1.05);
    osc.connect(g).connect(ctx.destination);
    osc.start(t0);
    lfo.start(t0);
    osc.stop(t0 + 1.05);
    lfo.stop(t0 + 1.05);
    this.noise(250, "bandpass", 800, 4000, 0.08, 650); // the wipe "zzzt"
  }
}
