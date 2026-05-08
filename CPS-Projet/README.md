# CPS-2026 : Système de Publication/Souscription en BCM4Java

> Projet CPS 2026 — PENG Kairui & CHU Feiyang

---

## Vue d'ensemble des tests CVM

Le projet contient **6 scénarios CVM** de complexité croissante, couvrant l'ensemble
des fonctionnalités du système de publication/souscription :

| CVM | Étape | Composants | Points clés |
|-----|-------|-----------|-------------|
| `CVM_Audit1` | 1 | 3 | Pub/Sub de base (FREE) |
| `CVM_Audit1_filtre` | 1 | 5 | Filtrage métier (WindmillFilter) |
| `CVM_PluginTest` | 2 | 6 | Plugins + 3 classes de service |
| `CVM_FilterTest` | 2 | 8 | TimeFilter + PropertiesFilter + destruction de canal |
| `CVM_ScenarioTest` | 2 | 47 | Scénario temporisé 45 clients, canaux dynamiques |
| `CVM_AsyncTest` | 3 | 16 | Réception avancée + parallélisme + asyncPublishAndNotify |

---

## 1. CVM_Audit1

**Objectif** : Vérifier le fonctionnement minimal de la publication/souscription pour
des clients FREE.

### Composants

| URI | Rôle | Classe de service | Canal |
|-----|------|-------------------|-------|
| Broker | Courtier | — | 3 canaux pré-créés |
| `client-subscriber-receiving` | Souscripteur | FREE | channel0 |
| `client-publisher-receiving` | Publieur | FREE | channel0 |

### Filtre

- `MessageFilter(null, null, null)` — accepte tous les messages.

### Déroulement

```
deploy → start → execute (5s)
  1. Subscriber s'enregistre, souscrit à channel0
  2. Publisher s'enregistre, publie un message "demo" sur channel0
  3. Subscriber reçoit le message
```

### Ce qui est vérifié

- Enregistrement FREE, connexion aux ports du courtier
- Publication et réception synchrones basiques

---

## 2. CVM_Audit1_filtre

**Objectif** : Démontrer le filtrage métier avec le `WindmillFilter` sur des données
météorologiques réelles (WindData, MeteoAlert).

### Composants

| URI | Rôle | Type | Classe de service | Canal |
|-----|------|------|-------------------|-------|
| Broker | Courtier | — | — | 3 canaux |
| `windmill-receiving` | Souscripteur | Windmill_before | FREE | channel0 |
| `station1-receiving` | Publieur | MeteoStation_before | FREE | channel0 |
| `station2-receiving` | Publieur | MeteoStation_before | FREE | channel0 |
| `office-receiving` | Publieur | MeteoOffice_before | FREE | channel0 |

### Messages

| Publieur | Payload | Propriétés |
|----------|---------|------------|
| station1 | `WindData(2.5, 48.8)` force=42 | type=wind, speed=42.0 |
| station2 | `WindData(3.0, 47.5)` force=10 | type=wind, speed=10.0 |
| office | `MeteoAlert(STORM, ORANGE)` | type=alert, level=orange |

### Filtre

- `WindmillFilter` — filtre métier spécialisé pour les éoliennes.

### Déroulement

```
1. Windmill souscrit avec WindmillFilter
2. Trois publieurs envoient des messages sur channel0
3. Windmill reçoit uniquement les messages acceptés par le filtre
```

---

## 3. CVM_PluginTest

**Objectif** : Valider le mécanisme de plugins (greffons) pour l'enregistrement,
la publication et la souscription, ainsi que le fonctionnement avec les 3 classes
de service (FREE / STANDARD / PREMIUM).

### Composants

| URI | Rôle | Type | Classe de service | Canal | Filtre |
|-----|------|------|-------------------|-------|--------|
| Broker | Courtier | — | — | 2 canaux | — |
| `windmill-1` | Souscripteur | Windmill | FREE | channel0 | speed > 30 |
| `desk-1` | Souscripteur | Windmill | PREMIUM | channel1 | type = "alert" |
| `station-1` | Publieur | MeteoStation | FREE | channel0 | — |
| `station-2` | Publieur | MeteoStation | STANDARD | channel0 | — |
| `office-1` | Publieur | MeteoOffice | PREMIUM | channel1 | — |

### Messages

| Publieur | Payload | force |
|----------|---------|-------|
| station-1 | WindData north | 42 (> 30 → passe le filtre) |
| station-2 | WindData south | 12 (< 30 → filtré) |
| office-1 | MeteoAlert STORM ORANGE | — |

### Déroulement

```
1. Tous les composants s'enregistrent via leurs plugins
2. Windmill-1 souscrit à channel0 (strongWindFilter: speed > 30)
   → reçoit station-1 (42 kt), ne reçoit PAS station-2 (12 kt)
   → mise à jour orientation: (0, -1) — face au vent nord
3. Desk-1 souscrit à channel1 (alertFilter: type = "alert")
   → reçoit l'alerte ORANGE
   → position (0,0) hors région affectée → pas d'arrêt de sécurité
```

### Ce qui est vérifié

- Plugins d'enregistrement, publication, souscription
- PropertyFilter (exact match, GreaterThan)
- Trois classes de service
- Logique applicative Windmill (orientation + vérification de région)

---

## 4. CVM_FilterTest

**Objectif** : Démontrer les trois types de filtres avancés (TimeFilter,
PropertiesFilter) et la destruction dynamique de canal.

### Composants

| URI | Rôle | Type | Classe | Canal | Filtre |
|-----|------|------|--------|-------|--------|
| Broker | Courtier | — | — | 1 canal | — |
| `time-filter-sub` | Souscripteur | Windmill | FREE | channel0 | **TimeFilter.AfterFilter** (> now−60s) |
| `props-filter-sub` | Souscripteur | Windmill | FREE | channel0 | **PropertiesFilter** (type=wind AND zone=north) |
| `temp-sub` | Souscripteur | Windmill | STANDARD | temp-channel | match-all |
| `station-recent-north` | Publieur | MeteoStation | FREE | channel0 | — |
| `station-recent-south` | Publieur | MeteoStation | FREE | channel0 | — |
| `station-old-north` | Publieur | MeteoStation | FREE | channel0 | — |
| `temp-destroyer` | Créateur + Publieur + Destructeur | Client | PREMIUM | temp-channel | — |

### Messages

| Publieur | Timestamp | zone | speed |
|----------|-----------|------|-------|
| station-recent-north | **now** | north | 42 |
| station-recent-south | **now** | south | 38 |
| station-old-north | **now − 2 jours** | north | 35 |
| temp-destroyer | now | — | — |

### Scénario temporisé (accélération 60x)

```
 t+3s    temp-destroyer crée temp-channel
 t+10s   temp-sub souscrit à temp-channel
 t+15s   time-filter-sub et props-filter-sub souscrivent à channel0
 t+25s   3 stations publient sur channel0
 t+30s   temp-destroyer publie sur temp-channel
 t+40s   temp-destroyer détruit temp-channel
```

### Résultats attendus

| Souscripteur | recent-north | recent-south | old-north | Raison |
|-------------|:---:|:---:|:---:|--------|
| time-filter-sub | **reçu** | **reçu** | filtré | TimeFilter : timestamp 2j ancien < seuil |
| props-filter-sub | **reçu** | filtré | **reçu** | PropertiesFilter : zone≠north filtré ; pas de contrainte temps |
| temp-sub | — | — | — | reçoit le message de temp-destroyer avant destruction |

### Ce qui est vérifié

- `TimeFilter.AfterFilter`
- `PropertiesFilter` + `MultiValuesFilter` (contrainte croisée)
- Création / publication / destruction de canal par un client PREMIUM
- Logique applicative Windmill : `wind data discarded: age > 3600s`

---

## 5. CVM_ScenarioTest

**Objectif** : Scénario temporisé à grande échelle (45 clients) couvrant
l'ensemble des fonctionnalités : 3 classes de service, canaux dynamiques,
filtres, composants métier.

### Composants (45 clients)

| Groupe | URIs | Type | Classe | Canal | Filtre | Nombre |
|--------|------|------|--------|-------|--------|--------|
| Souscripteurs | windmill-1..10 | Windmill | FREE | channel0 | speed > 30 | 10 |
| Souscripteurs | desk-1..8 | Windmill | PREMIUM | channel1 | type = "alert" | 8 |
| Souscripteurs | monitor-1..5 | Windmill | STANDARD | channel2 | match-all | 5 |
| Souscripteurs | sensor-1..5 | Windmill | STANDARD | channel3 | match-all | 5 |
| Publieurs | station-1..8 | MeteoStation | FREE | channel0 | — | 8 |
| Publieurs | office-1..4 | MeteoOffice | PREMIUM | channel1 | — | 4 |
| Publieurs | data-1..2 | MeteoStation | STANDARD | channel2 | — | 2 |
| Publieurs | report-1 | MeteoStation | STANDARD | channel3 | — | 1 |
| Créateurs | premium-creator-1/2 | Client | PREMIUM | channel2/3 | — | 2 |

### Messages

- **channel0** : 4 vents forts (35–52 kt, passent strongWindFilter) + 4 vents faibles (5–22 kt, filtrés)
- **channel1** : 4 alertes (orange/red/yellow/orange)
- **channel2** : 2 données capteur + 1 annonce créateur
- **channel3** : 1 rapport + 1 annonce créateur

### Scénario temporisé (accélération 60x)

```
 t+5s    premium-creator-1/2 créent channel2 et channel3
 t+20s   10 windmills souscrivent à channel0
 t+25s   8 desks souscrivent à channel1
 t+30s   5 monitors souscrivent à channel2
 t+33s   5 sensors souscrivent à channel3
 t+40s   8 stations publient sur channel0
 t+45s   4 offices publient sur channel1
 t+50s   data-1/2 + report-1 publient sur channel2/3
 t+55s   premium-creator-1/2 publient sur channel2/3
```

### Ce qui est vérifié

- 45 composants en parallèle
- Création dynamique de canaux (PREMIUM)
- Filtrage sélectif (strongWind, alert, match-all)
- Trois classes de service (FREE, STANDARD, PREMIUM)
- Logique applicative Windmill sur tous les windmills (orientation)

---

## 6. CVM_AsyncTest

**Objectif** : Démontrer les modes de réception avancés (§3.5.3), la publication
asynchrone avec notification (§3.6.2), le parallélisme avec pools de threads
différenciés par classe de service (§3.6.3), et la résistance à la pression
concurrente.

### Composants (14 clients)

| URI | Rôle | Type | Classe | Mode de réception / publication |
|-----|------|------|--------|------|
| wait-sub-1 | Souscripteur | Windmill | FREE | `waitForNextMessage` (bloquant) |
| wait-sub-2 | Souscripteur | Windmill | STANDARD | `waitForNextMessage` |
| wait-sub-3 | Souscripteur | Windmill | PREMIUM | `waitForNextMessage` |
| future-sub-1 | Souscripteur | Windmill | STANDARD | `getNextMessage` (Future) |
| future-sub-2 | Souscripteur | Windmill | PREMIUM | `getNextMessage` (Future) |
| normal-sub-1 | Souscripteur | Windmill | FREE | receive standard |
| normal-sub-2 | Souscripteur | Windmill | FREE | receive standard |
| normal-sub-3 | Souscripteur | Windmill | STANDARD | receive standard |
| pub-1 | Publieur | MeteoStation | FREE | `publish` (synchrone) |
| pub-2 | Publieur | MeteoStation | FREE | `publish` (synchrone) |
| pub-3 | Publieur | MeteoStation | FREE | `publish` (synchrone) |
| pub-4 | Publieur | MeteoStation | STANDARD | `asyncPublishAndNotify` |
| pub-5 | Publieur | MeteoStation | PREMIUM | `asyncPublishAndNotify` |
| pub-late | Publieur | MeteoStation | FREE | `publish` (synchrone, tardif) |

### Scénario temporisé (accélération 60x)

```
 t+5s    8 souscripteurs s'abonnent à channel0
 t+12s   wait-sub-1/2/3 appellent waitForNextMessage → bloquent
         future-sub-1/2 appellent getNextMessage → reçoivent des Futures
 t+20s   5 publications SIMULTANÉES (3 sync + 2 async)
         → pression concurrente sur le courtier
         → premier message par souscripteur dispatché au waiter/future
         → messages restants → receive standard
 t+30s   pub-late publie → tous les waiters consommés → receive standard
```

### Résultat numérique attendu

- **5 dispatched to waiter** = 3 waitForNextMessage + 2 getNextMessage
- **43 receive standard** = 6 messages × 8 souscripteurs − 5 consommés par waiters

### Pools de livraison par classe de service (BCM executor services)

| Classe | Executor Service URI | Threads |
|--------|---------------------|---------|
| PREMIUM | `delivery-premium-pool` | 4 |
| STANDARD | `delivery-standard-pool` | 2 |
| FREE | `delivery-free-pool` | 1 |

### Ce qui est vérifié

- `waitForNextMessage` : 3 appels concurrents résolus correctement
- `getNextMessage` : 2 Futures résolus correctement
- Dispatch waiter-first FIFO
- `asyncPublishAndNotify` avec port de notification (`AbnormalTerminationNotificationCI`)
- 5 publications concurrentes sur le même courtier (thread-safe)
- Différenciation des performances par classe de service

---

## 7. CVM_GossipTest

**Objectif** : Valider le protocole de bavardage (gossip) en mono-JVM avec
2 courtiers interconnectés.

### Composants

| URI | JVM | Rôle | Classe | Canal | Broker |
|-----|-----|------|--------|-------|--------|
| Broker-A | local | Courtier | — | 2 canaux | gossip → Broker-B |
| Broker-B | local | Courtier | — | 2 canaux | gossip → Broker-A |
| `windmill-on-B` | local | Souscripteur | FREE | channel0 | Broker-B |
| `station-on-A` | local | Publieur | FREE | channel0 | Broker-A |

### Déroulement

```
1. Broker-A et Broker-B démarrent, chacun pré-crée channel0/channel1
2. Connexion gossip bidirectionnelle (A↔B)
3. windmill-on-B souscrit à channel0 sur Broker-B
4. station-on-A publie WindData(force=40.3) sur channel0 via Broker-A
5. Broker-A propage le message via gossip à Broker-B
6. Broker-B délivre le message à windmill-on-B
7. windmill-on-B calcule l'orientation : (-0.868, -0.496)
```

### Ce qui est vérifié

- Publication cross-broker via protocole gossip
- `GossipMessage` : URI, timestamp, emitterURI, copyWithNewEmitterURI
- Déduplication (putIfAbsent sur processedGossipURIs)
- Intégration transparente : le client ne sait pas qu'un autre broker existe

---

## 8. DistributedCVM (5 JVMs)

**Objectif** : Déploiement réparti sur 5 machines virtuelles Java avec topologie
gossip en anneau + raccourci (non full-mesh), démontrant la propagation multi-hop,
la déduplication, et les 3 classes de service.

### Topologie gossip

```
    Broker-A ---- Broker-B
       |             |
    Broker-E ---- Broker-C
         \        /
          Broker-D
```

| Broker | JVM | Voisins gossip | Hops depuis A |
|--------|-----|---------------|---------------|
| A | jvm1 | B, E | 0 |
| B | jvm2 | A, C | 1 |
| C | jvm3 | B, E, D | 2 (via B) ou 2 (via E) |
| D | jvm4 | C, E | 2 (via E) ou 3 (via B→C) |
| E | jvm5 | A, C, D | 1 |

### Composants (20 clients répartis sur 5 JVMs)

| JVM | Broker | Composants | Classe | Canal |
|-----|--------|-----------|--------|-------|
| jvm1 | A | 3 MeteoStation (station-A1/A2/A3) | FREE | channel0 (pub) |
| jvm2 | B | 4 Windmill (windmill-B1..B4) | FREE | channel0 (sub, speed>30) |
| jvm3 | C | 1 MeteoOffice (office-C1) + 1 MeteoStation (station-C1) | PREMIUM/FREE | channel1 (pub) + channel0 (pub) |
| jvm4 | D | 2 Windmill (monitor-D1/D2) + 1 Windmill (desk-D1) | STANDARD/PREMIUM | channel0 (sub) + channel1 (sub) |
| jvm5 | E | 3 Windmill (windmill-E1..E3) + 2 Windmill (desk-E1/E2) + 1 MeteoStation (station-E1) | FREE/PREMIUM/FREE | channel0 (sub) + channel1 (sub) + channel0 (pub, weak) |

### Messages

| Source | Canal | Contenu | force | Passe strongWindFilter? |
|--------|-------|---------|-------|------------------------|
| station-A1 (jvm1) | ch0 | WindData north | 45 | oui |
| station-A2 (jvm1) | ch0 | WindData east | 38 | oui |
| station-A3 (jvm1) | ch0 | WindData west | 52 | oui |
| station-C1 (jvm3) | ch0 | WindData north | 40 | oui |
| station-E1 (jvm5) | ch0 | WindData south | **10** | **non (filtré)** |
| office-C1 (jvm3) | ch1 | MeteoAlert STORM ORANGE | — | — |

### Propagation vérifiée

**channel0 (vent) — multi-hop :**
```
station-A1/A2/A3 (jvm1/Broker-A)
  → Broker-B (jvm2) : windmill-B1..B4 reçoivent (1 hop)
  → Broker-E (jvm5) : windmill-E1..E3 reçoivent (1 hop)
  → Broker-C (jvm3) : station-C1 reçoit (2 hops via B ou E)
  → Broker-D (jvm4) : monitor-D1/D2 reçoivent (2 hops via E→D ou B→C→D)
```

**channel1 (alerte) — multi-hop :**
```
office-C1 (jvm3/Broker-C)
  → Broker-D (jvm4) : desk-D1 → SAFETY STOP (1 hop)
  → Broker-E (jvm5) : desk-E1/E2 → SAFETY STOP (1 hop)
  → Broker-B → Broker-A (2 hops, pas de subscribers sur A/B pour ch1)
```

**Filtrage vérifié :**
- station-E1 (speed=10) publié sur channel0 — aucun windmill ne le reçoit (filtré par strongWindFilter >30)

**Déduplication vérifiée :**
- Broker-D reçoit les messages de A via deux chemins (A→E→D et A→B→C→D), mais chaque message n'est traité qu'une seule fois

### Lancement (VSCode)

```
1. Lancer "GlobalRegistry"          — attendre 1-2s
2. Lancer "CyclicBarrier"           — attendre 1-2s
3. Lancer "2) Distributed JVMs"     — 5 JVMs simultanément
```

Ou en ligne de commande :
```bash
# Terminal 1 : GlobalRegistry
java -cp "..." fr.sorbonne_u.components.registry.GlobalRegistry CPS-Projet/config.xml

# Terminal 2 : CyclicBarrier
java -cp "..." fr.sorbonne_u.components.cvm.utils.DCVMCyclicBarrier CPS-Projet/config.xml

# Terminaux 3-7 : JVMs
java -cp "..." fr.sorbonne_u.publication.DistributedCVM jvm1 CPS-Projet/config.xml
java -cp "..." fr.sorbonne_u.publication.DistributedCVM jvm2 CPS-Projet/config.xml
java -cp "..." fr.sorbonne_u.publication.DistributedCVM jvm3 CPS-Projet/config.xml
java -cp "..." fr.sorbonne_u.publication.DistributedCVM jvm4 CPS-Projet/config.xml
java -cp "..." fr.sorbonne_u.publication.DistributedCVM jvm5 CPS-Projet/config.xml
```

### Ce qui est vérifié

- Protocole gossip sur topologie non-full-mesh (anneau + raccourci)
- Propagation multi-hop (A→B→C→D, A→E→D)
- Déduplication atomique (`putIfAbsent` sur `processedGossipURIs`)
- Filtrage emitter (ne renvoie pas au voisin qui a émis le message)
- 3 classes de service avec pools de threads différenciés (FREE/STANDARD/PREMIUM)
- Filtrage applicatif cross-JVM (strongWindFilter, alertFilter)
- Arrêt de sécurité des éoliennes via alerte cross-JVM
- Chaque JVM contient un broker local + ses clients (§3.7.4)
- 5 JVMs (> 3 minimum requis par le cahier des charges)

---

## 9. DistributedScenarioCVM (3 JVMs, temporisé)

**Objectif** : Scénario de test temporisé réparti sur 3 JVMs — satisfait
simultanément les deux exigences du §4.5 point 3 : *scénario temporisé*
et *déploiement sur ≥3 machines virtuelles Java*.

### Topologie gossip

```
    Broker-1 ---- Broker-2
        \          /
         Broker-3
```

### Composants (10 clients répartis sur 3 JVMs)

| JVM | Broker | Composants | Classe | Canal |
|-----|--------|-----------|--------|-------|
| jvm1 | Broker-1 | ClocksServer + station-1, station-2 (pub ch0) + office-1 (pub ch1) | FREE / PREMIUM | channel0, channel1 |
| jvm2 | Broker-2 | windmill-1/2/3 (sub ch0, strongWindFilter) + creator-1 (crée storm-alerts, pub RED) | FREE / PREMIUM | channel0, storm-alerts |
| jvm3 | Broker-3 | desk-1 (sub ch1) + desk-2 (sub storm-alerts) + station-3 (pub ch0, weak wind) | PREMIUM / FREE | channel1, storm-alerts, channel0 |

### Scénario temporisé (accélération 60x)

| Temps | JVM | Action | Fonctionnalité démontrée |
|-------|-----|--------|--------------------------|
| t+5s | JVM2 | creator-1 crée `storm-alerts` | Création dynamique de canal + gossip CREATE\_CHANNEL |
| t+15s | JVM2 | windmill-1/2/3 souscrivent à channel0 | Souscription locale |
| t+15s | JVM3 | desk-1 souscrit à channel1, desk-2 souscrit à storm-alerts | Souscription à canal dynamique propagé par gossip |
| t+25s | JVM1 | station-1/2 publient vent fort (45kt, 38kt) sur channel0 | Publication cross-JVM via gossip (JVM1→JVM2) |
| t+30s | JVM1 | office-1 publie alerte ORANGE sur channel1 | Alerte cross-JVM via gossip (JVM1→JVM3) |
| t+35s | JVM2 | creator-1 publie alerte RED sur storm-alerts | Publication sur canal dynamique cross-JVM (JVM2→JVM3) |
| t+45s | JVM3 | station-3 publie vent faible 10kt sur channel0 | Filtrage : strongWindFilter rejette (speed ≤ 30) |

### Résultats attendus

- **windmill-1/2/3** (JVM2) reçoivent les vents forts de JVM1 (45kt, 38kt) via gossip, calculent l'orientation
- **windmill-1/2/3** ne reçoivent **pas** le vent faible de station-3 (10kt, filtré)
- **desk-1** (JVM3) reçoit l'alerte ORANGE de JVM1 → arrêt de sécurité
- **desk-2** (JVM3) reçoit l'alerte RED de JVM2 via canal dynamique storm-alerts → arrêt de sécurité
- Coordination temporelle assurée par ClocksServer partagé (JVM1, accessible par RMI)

### Lancement (VSCode)

```
1. Lancer "GlobalRegistry-3jvm"               — attendre 2s
2. Lancer "CyclicBarrier-3jvm"                 — attendre 2s
3. Lancer "4) Scenario 3-JVM (after 3jvm infra)" — 3 JVMs simultanément
```

### Ce qui est vérifié

- **§4.5 point 3** : scénario temporisé + ≥3 JVMs + application météo/éolienne
- Création dynamique de canal en mode distribué (storm-alerts : JVM2 → gossip → JVM3)
- Publication cross-JVM temporisée (horloge accélérée 60x partagée)
- Filtrage applicatif cross-JVM (strongWindFilter)
- Arrêt de sécurité des éoliennes via alerte cross-JVM
- 3 classes de service (FREE, STANDARD, PREMIUM) sur 3 JVMs distinctes

---

## Récapitulatif des couvertures par étape

| Fonctionnalité (§ cahier des charges) | Tests |
|--------------------------------------|-------|
| §2 Messages et filtres | CVM_Audit1, CVM_Audit1_filtre, CVM_FilterTest |
| §3.1 Clients FREE | CVM_Audit1, CVM_PluginTest |
| §3.2 Clients privilégiés | CVM_PluginTest, CVM_ScenarioTest |
| §3.4 Application météo/éolienne | CVM_Audit1_filtre, CVM_PluginTest, CVM_ScenarioTest, DistributedCVM, DistributedScenarioCVM |
| §3.5 Greffons (plugins) | CVM_PluginTest, CVM_ScenarioTest, CVM_AsyncTest |
| §3.5.3 Réception avancée | CVM_AsyncTest |
| §3.6.2 asyncPublishAndNotify | CVM_AsyncTest |
| §3.6.3 Pools de threads | CVM_AsyncTest, CVM_ScenarioTest, DistributedCVM |
| §3.7 Gossip / répartition | CVM_GossipTest, DistributedCVM, DistributedScenarioCVM |
| §3.7.4 Déploiement ≥3 JVMs | DistributedCVM (5 JVMs), DistributedScenarioCVM (3 JVMs) |
| §4.5.3 Scénario temporisé + ≥3 JVMs | **DistributedScenarioCVM** |
