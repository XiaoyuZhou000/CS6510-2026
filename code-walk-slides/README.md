# Pipeline Analytics Code Walk

Slidev deck for the CS6510 pipeline-architecture code walk.

## Run locally

```powershell
npm install
npm run dev
```

## Build and export

```powershell
npm run build
npm run export:pdf
```

The npm scripts automatically work around Slidev's Windows Unicode-path issue by creating a temporary drive mapping while the command runs.

The deck is designed for a 15–20 minute presentation. Presenter notes are embedded at the end of each slide in `slides.md`.
