#!/usr/bin/env node

/**
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  COPIE AUTOMATIQUE DES AFFECTATIONS TVA MENSUELLE (EM)                  ║
 * ╠═══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                         ║
 * ║  BUT :                                                                  ║
 * ║    Pour chaque client ayant un régime TVA "EM" (mensuel),               ║
 * ║    ce script duplique toutes les affectations (Assignment) du mois N    ║
 * ║    vers le mois N+1, en recalculant les dates d'échéance.               ║
 * ║                                                                         ║
 * ║  FLUX :                                                                 ║
 * ║    1. Charger le .env (DATABASE_URL)                                    ║
 * ║    2. Trouver les exercices fiscaux ayant TVA EM pour le mois source    ║
 * ║    3. Pour chaque exercice → récupérer les affectations du mois source  ║
 * ║    4. Pour chaque affectation → vérifier si elle existe déjà en cible   ║
 * ║    5. Si non → calculer l'échéance + insérer en base                    ║
 * ║                                                                         ║
 * ║  USAGE :                                                                ║
 * ║    node index.js                  → Exécution réelle (mois courant)     ║
 * ║    node index.js --dry-run        → Simulation sans écriture            ║
 * ║    node index.js --periode 3      → Forcer le mois source (ex: mars)    ║
 * ║                                                                         ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 */

const mysql = require('mysql2/promise');
const axios = require('axios');
const path = require('path');
const fs = require('fs');

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  1. CONFIGURATION – Chargement du .env et connexion DB                   │
// └───────────────────────────────────────────────────────────────────────────┘

/**
 * Charge les variables du fichier .env (situé dans le même dossier que ce script).
 * Ne remplace PAS les variables déjà définies dans process.env.
 */
function loadEnv() {
  const envPath = path.resolve(__dirname, '.env');

  if (!fs.existsSync(envPath)) {
    console.error('Fichier .env introuvable à', envPath);
    process.exit(1);
  }

  const lines = fs.readFileSync(envPath, 'utf-8').split('\n');

  for (const line of lines) {
    const trimmed = line.trim();

    // Ignorer les lignes vides et les commentaires
    if (!trimmed || trimmed.startsWith('#')) continue;

    // Supporter les deux formats : KEY=value et KEY: value
    let separatorIdx = trimmed.indexOf('=');
    if (separatorIdx === -1) separatorIdx = trimmed.indexOf(':');
    if (separatorIdx === -1) continue;

    const key = trimmed.slice(0, separatorIdx).trim();
    let val = trimmed.slice(separatorIdx + 1).trim();

    // Retirer les guillemets encadrants
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }

    process.env[key] = val;
  }
}

/**
 * Parse une URL MySQL du type : mysql://user:password@host:port/database
 * @returns {{ user, password, host, port, database }}
 */
function parseDatabaseUrl(url) {
  const match = url.match(/^mysql:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/([^?]+)/);

  if (!match) {
    console.error('❌ Format DATABASE_URL invalide :', url);
    process.exit(1);
  }

  return {
    user: match[1],
    password: match[2],
    host: match[3],
    port: parseInt(match[4], 10),
    database: match[5],
  };
}

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  2. JOURS FÉRIÉS – Calcul des jours non ouvrés                 │
// └───────────────────────────────────────────────────────────────────────────┘

/**
 * Calcule le lundi de Pâques pour une année donnée (algorithme de Meeus).
 */
function calculateEasterMonday(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31) - 1;
  const day = ((h + l - 7 * m + 114) % 31) + 1;

  const easterSunday = new Date(year, month, day);
  const easterMonday = new Date(easterSunday);
  easterMonday.setDate(easterMonday.getDate() + 1);
  return easterMonday;
}

/**
 * Retourne la liste de tous les jours fériés français pour une année.
 * Inclut : fêtes fixes + lundi de Pâques + Ascension + lundi de Pentecôte.
 */
function getFrenchHolidays(year) {
  // Fêtes fixes
  const holidays = [
    new Date(year, 0, 1),    // 1er janvier    – Jour de l'an
    new Date(year, 4, 1),    // 1er mai        – Fête du travail
    new Date(year, 4, 8),    // 8 mai          – Victoire 1945
    new Date(year, 6, 14),   // 14 juillet     – Fête nationale
    new Date(year, 7, 15),   // 15 août        – Assomption
    new Date(year, 10, 1),   // 1er novembre   – Toussaint
    new Date(year, 10, 11),  // 11 novembre    – Armistice 1918
    new Date(year, 11, 25),  // 25 décembre    – Noël
  ];

  // Fêtes mobiles (basées sur Pâques)
  const easterMonday = calculateEasterMonday(year);
  holidays.push(easterMonday);                                          // Lundi de Pâques

  const ascension = new Date(easterMonday);
  ascension.setDate(ascension.getDate() + 38);                          // Ascension
  holidays.push(ascension);

  const pentecostMonday = new Date(easterMonday);
  pentecostMonday.setDate(pentecostMonday.getDate() + 49);              // Lundi de Pentecôte
  holidays.push(pentecostMonday);

  return holidays;
}

/** Vérifie si une date tombe un jour férié français. */
function isFrenchHoliday(date) {
  return getFrenchHolidays(date.getFullYear()).some(
    (h) =>
      h.getDate() === date.getDate() &&
      h.getMonth() === date.getMonth() &&
      h.getFullYear() === date.getFullYear(),
  );
}

/** Vérifie si une date tombe un samedi ou dimanche. */
function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

/**
 * Avance une date au prochain jour ouvré (ni week-end, ni férié).
 * Si la date est déjà un jour ouvré, elle est retournée telle quelle.
 */
function adjustToNextWorkingDay(date) {
  const result = new Date(date);
  while (isWeekend(result) || isFrenchHoliday(result)) {
    result.setDate(result.getDate() + 1);
  }
  return result;
}

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  3. CALCUL DE L'ÉCHÉANCE TVA EM                                         │
// │                                                                          │
// │  Les dates limites de dépôt dépendent de :                               │
// │    - la forme juridique (EI, SARL, SAS, ASS…)                            │
// │    - la localisation (Paris/RP vs province)                               │
// │    - le SIREN ou la 1ère lettre du nom du dirigeant                      │
// │                                                                          │
// │  Référence : calendrier fiscal des entreprises (impots.gouv.fr)          │
// └───────────────────────────────────────────────────────────────────────────┘

/** Étapes dont l'échéance est calculée automatiquement. */
const STEPS_WITH_DUE_DATE = [
  'Contrôle & validation',
  'Télétransmission',
  'Suivi & clôture',
];

/**
 * Calcule la date d'échéance TVA EM pour un client donné.
 *
 * @param {string} legalFormCode  - Forme juridique (EI, SARL, SAS, ASS…)
 * @param {string} postalCode     - Code postal du client
 * @param {string} siret          - Numéro SIRET (14 chiffres)
 * @param {string} directorName   - Nom du dirigeant (pour EI)
 * @param {Date}   periodEndDate  - Fin de la période TVA cible
 * @returns {Date|null}           - Date d'échéance ajustée, ou null
 */
function calculateDueDate(legalFormCode, postalCode, siret, directorName, periodEndDate) {
  if (!periodEndDate) return null;

  const periodEnd = new Date(periodEndDate);

  // Le dépôt se fait le mois suivant la fin de période
  const depositMonth = new Date(periodEnd.getFullYear(), periodEnd.getMonth() + 1, 1);
  const year = depositMonth.getFullYear();
  const month = depositMonth.getMonth();

  // Paris / Île-de-France = départements 75, 92, 93, 94
  const isParisRegion = postalCode && ['75', '92', '93', '94'].some((p) => postalCode.startsWith(p));

  /** Crée une date au jour donné, ajustée au prochain jour ouvré. */
  const dueOn = (day) => adjustToNextWorkingDay(new Date(year, month, day));

  // ── EI (Entreprise Individuelle) ──────────────────────────────────────
  if (legalFormCode === 'EI') {
    const firstLetter = directorName ? directorName.charAt(0).toUpperCase() : 'A';
    const nameAtoH = firstLetter >= 'A' && firstLetter <= 'H';

    if (isParisRegion) {
      if (nameAtoH) {
        const firstDigit = siret ? parseInt(siret.charAt(0), 10) : 0;
        return dueOn(firstDigit % 2 === 0 ? 15 : 16);
      }
      return dueOn(17);
    }
    return dueOn(nameAtoH ? 16 : 19);
  }

  // ── SARL / EURL / SNC ────────────────────────────────────────────────
  if (['SARL', 'EURL', 'SNC'].includes(legalFormCode || '')) {
    const siren = siret ? parseInt(siret.substring(0, 9), 10) : 0;

    if (isParisRegion) {
      if (siren <= 68999999) return dueOn(19);
      if (siren <= 78999999) return dueOn(20);
      return dueOn(21);
    }
    return dueOn(21);
  }

  // ── SA / SAS / SASU ──────────────────────────────────────────────────
  if (['SA', 'SAS', 'SASU'].includes(legalFormCode || '')) {
    const siren = siret ? parseInt(siret.substring(0, 9), 10) : 0;

    if (isParisRegion) {
      return dueOn(siren <= 74999999 ? 23 : 24);
    }
    return dueOn(24);
  }

  // ── ASS (Association) ────────────────────────────────────────────────
  if (legalFormCode === 'ASS') {
    return dueOn(24);
  }

  // Forme juridique inconnue → pas d'échéance automatique
  return null;
}

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  4. REQUÊTES SQL                                                         │
// └───────────────────────────────────────────────────────────────────────────┘

/**
 * Requête 1 : Trouver tous les exercices fiscaux en cours ayant TVA EM
 *             pour la période source, avec les infos du client.
 *
 * Params : [targetPeriode, sourcePeriode]
 */
const SQL_FIND_FISCAL_YEARS = `
  SELECT DISTINCT
    fy.id           AS fiscalYearId,
    fy.clientId,
    fy.startDate,
    fy.endDate,
    fy.managerId,
    c.name          AS clientName,
    c.siret,
    c.postalCode,
    c.legalFormCode,
    c.directorName,
    fyvr_target.startPeriode AS targetStartPeriode,
    fyvr_target.endPeriode   AS targetEndPeriode
  FROM FiscalYearVATRegime fyvr
  JOIN FiscalYear fy ON fy.id = fyvr.fiscalYearId
  JOIN Client c      ON c.id = fy.clientId
  LEFT JOIN FiscalYearVATRegime fyvr_target
    ON  fyvr_target.fiscalYearId = fy.id
    AND fyvr_target.periode = ?
  WHERE fyvr.vatRegimeCode = 'EM'
    AND fyvr.periode = ?
    AND fy.isCurrent = 1
`;

/**
 * Requête 2 : Récupérer les affectations TVA de la période source.
 *
 * Params : [fiscalYearId, sourcePeriode]
 */
const SQL_GET_ASSIGNMENTS = `
  SELECT
    a.taskId,
    a.fiscalYearId,
    a.vatPeriode,
    a.assignerId,
    a.assigneeId,
    a.reassigneeId,
    a.dueDate,
    t.name        AS taskName,
    s.name        AS stepName,
    s.id          AS stepId,
    assignee.email    AS assigneeEmail,
    assigner.username AS assignerName
  FROM Assignment a
  JOIN Task t        ON t.id = a.taskId
  JOIN Step s        ON s.id = t.stepId
  JOIN Mission m     ON m.code = s.missionCode
  LEFT JOIN User assignee  ON assignee.id = a.assigneeId
  LEFT JOIN User assigner  ON assigner.id = a.assignerId
  WHERE a.fiscalYearId = ?
    AND a.vatPeriode = ?
    AND m.code = 'TVA'
  ORDER BY s.id, t.id
`;

/**
 * Requête 3 : Vérifier si une affectation existe déjà pour la période cible.
 *
 * Params : [taskId, fiscalYearId, targetPeriode]
 */
const SQL_CHECK_EXISTS = `
  SELECT 1 FROM Assignment
  WHERE taskId = ? AND fiscalYearId = ? AND vatPeriode = ?
`;

/**
 * Requête 4 : Insérer une nouvelle affectation (copie).
 *
 * Params : [taskId, fiscalYearId, targetPeriode, assignerId, assigneeId, reassigneeId, dueDate]
 */
const SQL_INSERT_ASSIGNMENT = `
  INSERT INTO Assignment
    (taskId, fiscalYearId, vatPeriode, assignerId, assigneeId, reassigneeId,
     assignementDate, status, dueDate, accumulatedTime)
  VALUES (?, ?, ?, ?, ?, ?, NOW(), 'TODO', ?, 0)
`;

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  5. UTILITAIRES D'AFFICHAGE                                              │
// └───────────────────────────────────────────────────────────────────────────┘

const MONTHS_FR = [
  'janvier', 'février', 'mars', 'avril', 'mai', 'juin',
  'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre',
];

/** Numéro de mois (1-12) → nom français. */
function monthName(n) {
  return MONTHS_FR[n - 1] || 'mois ' + n;
}

/** Date → chaîne MySQL (YYYY-MM-DD HH:MM:SS). */
function toMySQLDate(date) {
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

/** Date → affichage français lisible (ex: "15 avril 2026"). */
function toFrenchDate(date) {
  return date.toLocaleDateString('fr-FR', { day: '2-digit', month: 'long', year: 'numeric' });
}

function printHeader(sourcePeriode, targetPeriode, dryRun) {
  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════════╗');
  console.log('║  COPIE DES AFFECTATIONS TVA MENSUELLE EM                     ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log('  📅 ' + monthName(sourcePeriode) + ' (période ' + sourcePeriode + ') → ' + monthName(targetPeriode) + ' (période ' + targetPeriode + ')');
  console.log('  🔧 Mode : ' + (dryRun ? '🔍 SIMULATION (dry-run) — aucune écriture' : '✏️  ÉCRITURE RÉELLE en base'));
  console.log('');
}

function printSummary(totalCopied, totalSkipped, totalErrors, totalNotified, sourcePeriode, targetPeriode, dryRun) {
  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════════╗');
  console.log('║  RÉSUMÉ                                                      ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log('  ✅ Copiées      : ' + totalCopied);
  console.log('  ⏭️  Ignorées     : ' + totalSkipped + ' (déjà existantes)');
  console.log('  ❌ Erreurs      : ' + totalErrors);
  console.log('  💬 Notifiées    : ' + totalNotified + ' (Slack)');
  console.log('  📅 ' + monthName(sourcePeriode) + ' → ' + monthName(targetPeriode));

  if (dryRun) {
    console.log('');
    console.log('  ℹ️  Mode dry-run : aucune modification effectuée.');
    console.log('     Relancez sans --dry-run pour exécuter réellement.');
  }

  console.log('');
}

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  6. NOTIFICATIONS SLACK                                                  │
// │                                                                          │
// │  Envoie un message direct (DM) à l'assignee via Slack                   │
// │  quand une nouvelle affectation est créée (mode réel uniquement).        │
// │                                                                          │
// │  Nécessite SLACK_BOT_TOKEN dans le .env.                                │
// │  Si absent ou en erreur → l'insertion se fait quand même.               │
// └───────────────────────────────────────────────────────────────────────────┘

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_API_URL = process.env.SLACK_API_URL || 'https://slack.com/api';

/** Cache email → Slack userId pour éviter des appels API répétés. */
const slackUserCache = new Map();

/**
 * Trouve le Slack userId à partir d'un email.
 * Utilise l'API users.lookupByEmail. Résultat mis en cache.
 */
async function getSlackUserIdByEmail(email) {
  if (!email || !SLACK_BOT_TOKEN) return null;
  if (slackUserCache.has(email)) return slackUserCache.get(email);

  try {
    const res = await axios.get(SLACK_API_URL + '/users.lookupByEmail', {
      params: { email },
      headers: { Authorization: 'Bearer ' + SLACK_BOT_TOKEN },
    });

    if (!res.data.ok) return null;

    const userId = res.data.user.id;
    slackUserCache.set(email, userId);
    return userId;
  } catch {
    return null;
  }
}

/**
 * Envoie un DM Slack à un utilisateur identifié par email.
 */
async function sendSlackDM(email, message) {
  const userId = await getSlackUserIdByEmail(email);
  if (!userId) return false;

  try {
    // Ouvrir la conversation DM
    const imRes = await axios.post(
      SLACK_API_URL + '/conversations.open',
      { users: userId },
      { headers: { Authorization: 'Bearer ' + SLACK_BOT_TOKEN, 'Content-Type': 'application/json' } },
    );

    const channelId = imRes.data.channel.id;

    // Envoyer le message
    await axios.post(
      SLACK_API_URL + '/chat.postMessage',
      { channel: channelId, text: message },
      { headers: { Authorization: 'Bearer ' + SLACK_BOT_TOKEN, 'Content-Type': 'application/json' } },
    );

    return true;
  } catch {
    return false;
  }
}

/**
 * Construit et envoie la notification Slack d'assignation.
 * Format identique à celui de ec-mars-api (SlackService).
 */
async function notifySlackAssignment({ taskName, assigneeEmail, assignerName, assignedAt, dueDate, clientName }) {
  const dateStr = assignedAt.toLocaleDateString('fr-FR') + ' à ' + assignedAt.toLocaleTimeString('fr-FR');

  let message =
    '*Assignation automatique de tache*\n\n' +
    '> *Tache :* ' + taskName + '\n' +
    '> *Dossier :* ' + clientName + '\n' +
    '> *Date d\'assignation :* ' + dateStr + '\n';

  if (dueDate) {
    message += '> *Date limite :* ' + dueDate.toLocaleDateString('fr-FR') + '\n';
  }

  return sendSlackDM(assigneeEmail, message);
}

// ┌───────────────────────────────────────────────────────────────────────────┐
// │  7. SCRIPT PRINCIPAL                                                     │
// └───────────────────────────────────────────────────────────────────────────┘

async function main() {
  // ── Lire les arguments CLI ────────────────────────────────────────────
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const periodeArgIdx = args.indexOf('--periode');
  const forcedPeriode = periodeArgIdx !== -1 ? parseInt(args[periodeArgIdx + 1], 10) : null;

  // ── Déterminer les périodes ────────────────────────────────────────────
  const sourcePeriode = forcedPeriode || (new Date().getMonth() + 1);
  const targetPeriode = sourcePeriode + 1;

  if (targetPeriode > 12) {
    console.log('⚠️  Période source = 12 (décembre). Pas de période 13.');
    console.log('   Les affectations de janvier doivent être créées manuellement.');
    process.exit(0);
  }

  printHeader(sourcePeriode, targetPeriode, dryRun);

  // ── Connexion à la base de données ────────────────────────────────────
  const dbConfig = parseDatabaseUrl(DATABASE_URL);

  console.log('  🔌 Connexion à ' + dbConfig.host + ':' + dbConfig.port + '/' + dbConfig.database + '…');

  const connection = await mysql.createConnection({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
  });

  console.log('  ✅ Connecté à la base de données\n');

  try {
    // ── ÉTAPE 1 : Trouver les exercices fiscaux TVA EM ──────────────────
    const [fiscalYears] = await connection.execute(SQL_FIND_FISCAL_YEARS, [targetPeriode, sourcePeriode]);

    if (fiscalYears.length === 0) {
      console.log('  ⚠️  Aucun exercice fiscal avec TVA EM trouvé pour ' + monthName(sourcePeriode));
      return;
    }

    console.log('  📋 ' + fiscalYears.length + ' exercice(s) avec TVA EM trouvé(s)\n');

    let totalCopied = 0;
    let totalSkipped = 0;
    let totalErrors = 0;
    let totalNotified = 0;

    // ── ÉTAPE 2 : Boucler sur chaque exercice fiscal ────────────────────
    for (const fy of fiscalYears) {
      console.log('  ┌─ 📁 ' + fy.clientName);
      console.log('  │  SIRET: ' + (fy.siret || '—') + ' | Forme: ' + (fy.legalFormCode || '—') + ' | CP: ' + (fy.postalCode || '—'));

      // Récupérer les affectations de la période source
      const [assignments] = await connection.execute(SQL_GET_ASSIGNMENTS, [fy.fiscalYearId, sourcePeriode]);

      if (assignments.length === 0) {
        console.log('  │  ⚠️  Aucune affectation en ' + monthName(sourcePeriode));
        console.log('  └────────────────────────────────────\n');
        continue;
      }

      console.log('  │  📦 ' + assignments.length + ' affectation(s) à copier\n');

      // ── ÉTAPE 3 : Copier chaque affectation ───────────────────────────
      for (const a of assignments) {
        // Vérifier si elle existe déjà en période cible
        const [existing] = await connection.execute(SQL_CHECK_EXISTS, [a.taskId, a.fiscalYearId, targetPeriode]);

        if (existing.length > 0) {
          console.log('  │  ⏭️  ' + a.taskName + ' → déjà existante');
          totalSkipped++;
          continue;
        }

        // Calculer l'échéance pour les étapes finales
        let dueDate = null;
        if (STEPS_WITH_DUE_DATE.includes(a.stepName) && fy.targetEndPeriode) {
          dueDate = calculateDueDate(fy.legalFormCode, fy.postalCode, fy.siret, fy.directorName, fy.targetEndPeriode);
        }

        const dueDateDisplay = dueDate ? toFrenchDate(dueDate) : '—';

        if (dryRun) {
          // Simulation
          console.log('  │  🔍 ' + a.taskName + ' → échéance: ' + dueDateDisplay + ' | assigné: #' + a.assigneeId);
          totalCopied++;
        } else {
          // Insertion réelle
          try {
            await connection.execute(SQL_INSERT_ASSIGNMENT, [
              a.taskId,
              a.fiscalYearId,
              targetPeriode,
              a.assignerId,
              a.assigneeId,
              a.reassigneeId || null,
              dueDate ? toMySQLDate(dueDate) : null,
            ]);

            console.log('  │  ✅ ' + a.taskName + ' → échéance: ' + dueDateDisplay);
            totalCopied++;

            // Notification Slack
            if (SLACK_BOT_TOKEN && a.assigneeEmail) {
              try {
                const sent = await notifySlackAssignment({
                  taskName: a.taskName,
                  assigneeEmail: a.assigneeEmail,
                  assignerName: a.assignerName || 'Système',
                  assignedAt: new Date(),
                  dueDate: dueDate || undefined,
                  clientName: fy.clientName,
                });
                if (sent) {
                  console.log('  │  💬 Slack → ' + a.assigneeEmail);
                  totalNotified++;
                }
              } catch {
                // Ne pas bloquer si Slack échoue
              }
            }
          } catch (err) {
            console.error('  │  ❌ ' + a.taskName + ' → ' + err.message);
            totalErrors++;
          }
        }
      }

      console.log('  └────────────────────────────────────\n');
    }

    // ── Résumé final ────────────────────────────────────────────────────
    printSummary(totalCopied, totalSkipped, totalErrors, totalNotified, sourcePeriode, targetPeriode, dryRun);

  } catch (err) {
    console.error('\n❌ Erreur fatale :', err.message);
    process.exit(1);
  } finally {
    await connection.end();
  }
}

// ── Initialisation ──────────────────────────────────────────────────────────
loadEnv();

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL non défini dans le .env');
  process.exit(1);
}

main();
