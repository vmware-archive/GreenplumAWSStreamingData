/**********************************************************************************************
 Copyright 2020 VMWare Inc
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *********************************************************************************************/

package io.greenplum.demo.streaming.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.Account;
import io.greenplum.demo.streaming.model.Bounds;
import io.greenplum.demo.streaming.model.Location;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.source.config.TransactionsSourceProperties;
import io.greenplum.demo.streaming.utils.StreamUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GlobalPosition;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
@Component
public class TransactionSimulator {
    private static final Log log = LogFactory.getLog(TransactionSimulator.class.getName());
    private final TransactionsSourceProperties appProperties;
    private List<Location> locations;
    private List<Account> accounts;
    private final ResourceLoader resourceLoader;
    private Map<String, List<Integer>> stateLocationIdsMap;
    private Map<String ,List<Location>> stateLocationsMap;
    private Map<String ,List<Location>> accountCloseLocations;
    private static final AtomicInteger tranIdAtomicInteger = new AtomicInteger((int)(System.currentTimeMillis()/1000));

    /**
     * Constructor
     * @param resourceLoader
     * @param appProperties
     */
    public TransactionSimulator(ResourceLoader resourceLoader, TransactionsSourceProperties appProperties) {
        this.resourceLoader = resourceLoader;
        this.appProperties = appProperties;
        this.init();
    }

    /**
     * Bootstrap the accounts, locations and mappings between them
     */
    public void init() {
        try {
            log.debug("Loading locations JSON file ....................");
            loadLocations();
            log.info("Locations Size = " + locations.size());
            log.debug("Loading accounts JSON file ....................");
            loadAccounts();
            log.info("Accounts Size = " + accounts.size());
            log.debug("Grouping locations per state ....................");
            loadStateLocations();
            log.debug("Mapping " + locations.size() + " locations to " + accounts.size() + accounts);
            locationWithinActVicinity();
        }catch (Exception e) {
            log.error("ERROR Initiating data. -> "+e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    public static Integer getNextTransactionId(){
        return tranIdAtomicInteger.incrementAndGet();
    }
    /**
     * Generates a batch of transactions based on configuration parameters.
     * @return List of {@link Transaction} objects.
     *
     */
    public List<Transaction> generateTransactions() {
        int fraudCounter = appProperties.getFraudForEvery();
        List<Transaction> transactions = new LinkedList<Transaction>();
        for(int counter=0; counter< appProperties.getTransactionsNumber(); counter++){
            boolean fraud = false;
            if (counter % fraudCounter == 0) {
                log.debug("***** Generating fraud record *****");
                fraud = true;
            }
            Transaction t = null;
            try {
                t = createTransaction(appProperties.getStoreFraudflag(), fraud);
            } catch (Exception e) {
                log.error("ERROR Generating transaction -> "+e.getMessage(), e);
                throw new RuntimeException(e);
            }
            transactions.add(t);
        }

        return transactions;
    }

    /**
     * Generates a batch of transactions based on configuration parameters.
     * @return JSON array with {@link Transaction} records
     */
    public String generateTransactionsJSON() {
        String transactionsJSON = null;
        try {
            List<Transaction> transactions = generateTransactions();
            transactionsJSON = getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(transactions);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return transactionsJSON;
    }

    public String getAccountState(String accountId) {
        String actState = null;
        //can be better
        for(int i =0; i< accounts.size(); i++) {
            if(accounts.get(i).getAccountId().equals(accountId))
                actState = accounts.get(i).getState();
        }
        return actState;
    }

    /**
     * load locations JSON file
     */
    private void loadLocations() throws Exception {
        ObjectMapper mapper = getObjectMapper();
        TypeReference<List<Location>> locationMapType;
        try {
            locationMapType = new TypeReference<List<Location>>() {};
            Resource resource = resourceLoader.getResource("classpath:locations.json");
            InputStream is = resource.getInputStream();
            this.locations = mapper.readValue(is, locationMapType);
        } catch (Exception e) {
            throw new RuntimeException("Error loading locations JSON file.", e);
        }
    }

    /**
     * group locations per state
     */
    private void loadStateLocations() throws Exception {
        stateLocationIdsMap = new HashMap<String, List<Integer>>();
        stateLocationsMap = new HashMap<String, List<Location>>();
        if(null == this.locations) {
            this.loadLocations();
        }
        locations.forEach(l -> {
            if (stateLocationIdsMap.containsKey(l.getMerchantState()) ){
                stateLocationIdsMap.get(l.getMerchantState()).add(l.getLocationId());
            }else {
                List<Integer> locationIds = new ArrayList<Integer>();
                locationIds.add(l.getLocationId());
                stateLocationIdsMap.put(l.getMerchantState(), locationIds);
            }
            if(stateLocationsMap.containsKey(l.getMerchantState()) ){
                stateLocationsMap.get(l.getMerchantState()).add(l);
            }else {
                List<Location> locations = new ArrayList<Location>();
                locations.add(l);
                stateLocationsMap.put(l.getMerchantState(), locations);
            }
        });

    }

    /**
     * load accounts JSON file
     */
    private void loadAccounts() throws Exception {
        ObjectMapper mapper = getObjectMapper();
        TypeReference<List<Account>> acctMapType;
        try {
            acctMapType = new TypeReference<List<Account>>() {};
            Resource resource = resourceLoader.getResource("classpath:accounts.json");
            InputStream is = resource.getInputStream();
            this.accounts = mapper.readValue(is, acctMapType);
        } catch (Exception e) {
            throw new RuntimeException("Error loading accounts JSON file.", e);
        }
    }

    /**
     * build list of locations within "distance" of account holders home address
     */
    private void locationWithinActVicinity() throws Exception {
        if ( this.accountCloseLocations == null ) {
            this.accountCloseLocations = new HashMap<String ,List<Location>> ();
        }
        this.accounts.forEach(a -> {
            this.locations.forEach(l -> {
                double dist = 0;
                try {
                    dist = findDistance(a.getLatitude(), a.getLongitude(), l.getMerchantLatitude(), l.getMerchantLongitude());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (dist < a.getTransactionRadius()){
                    l.setMerchantDistance(dist);
                    if (accountCloseLocations.containsKey(a.getAccountNumber()))
                        accountCloseLocations.get(a.getAccountNumber()).add(l);
                    }else {
                        List<Location> ll = new ArrayList<Location>();
                        accountCloseLocations.put(a.getAccountNumber(), ll);
                        accountCloseLocations.get(a.getAccountNumber()).add(l);
                    }
                });
        });
    }

    /**
     * Find the distance in miles between two Goe coordinates using
     * 'https://github.com/mgavaghan/geodesy.git'
     * @param actLat
     * @param actLong
     * @param merLat
     * @param merLong
     * @return Double
     */
    private Double findDistance (Double actLat, Double actLong,  Double merLat, Double merLong)
            throws Exception {
        GeodeticCalculator geoCalc = new GeodeticCalculator();
        Ellipsoid reference = Ellipsoid.WGS84;
        GlobalPosition merchantPos = new GlobalPosition(merLat, merLong, 0.0);
        GlobalPosition actPos = new GlobalPosition(actLat, actLong, 0.0);
        double distMeters = geoCalc.calculateGeodeticCurve(reference, actPos, merchantPos).getEllipsoidalDistance();
        double distMiles = distMeters/1609;
        return distMiles;
    }

    /**
     * select a location for transaction.
     * First look in the locations close to account home address and pick a random location.
     * If not found any location for the account address then pick a location from account state.
     * If none above match pick any location.
     * @param account
     * @return
     */
    private Location pickTransactionLocation(Account account) throws Exception
    {
        Location loc = null;
        // build list of locations within "distance" of account holders home address
        List<Location> closeLocations = this.accountCloseLocations.get(account.getAccountNumber());
        log.debug(closeLocations.size() + " total location(s) found within "
                + account.getTransactionRadius() +" miles");
        //locations found in close proximity so pick a random within those locations
        if (closeLocations != null && closeLocations.size() > 0 ) {
            loc = closeLocations.get(new Random().nextInt(closeLocations.size()));
        } else if(this.stateLocationIdsMap.keySet().contains(account.getState())) {
            //no locations found - looks within state
            log.debug("No merchant found within "+account.getTransactionRadius()
                    + " miles, so choosing location within state");
            List<Location> stateLocList = stateLocationsMap.get(account.getState());
            loc = stateLocList.get(new Random().nextInt(stateLocList.size()));
        } else {
            log.debug("No merchant found within "+account.getTransactionRadius()
                    + " miles or in state. So picking random location.");
            loc = this.locations.get(new Random().nextInt(this.locations.size()));
        }
        return loc;
    }

    /**
     * Create a Transaction record
     * @param storeFraudFlag
     * @return <code>{@link Transaction}</code>
     */
    private Transaction createTransaction (Boolean storeFraudFlag, boolean fraud)
            throws Exception {
        SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Account account = (Account) this.accounts.get(new Random().nextInt(this.accounts.size()));
        Location location = pickTransactionLocation(account);
        Double transactionAmount = null;
        Transaction transaction = new Transaction();
        transaction.setTransactionId(getNextTransactionId());
        transaction.setRlbLocationKey(location.getRlbLocationKey());
        transaction.setAccountNumber(account.getAccountNumber());
        transaction.setAccountId(account.getAccountId());
        transaction.setAccountLatitude(account.getLatitude());
        transaction.setAccountLongitude(account.getLongitude());
        transaction.setAccountState(account.getState());
        transaction.setCardType(account.getCardType());
        transaction.setLocationId(location.getLocationId());
        transaction.setMerchantName(location.getMerchantName());
        transaction.setMerchantCity(location.getMerchantCity());
        transaction.setMerchantCityAlias(location.getMerchantCityAlias());
        transaction.setMerchantState(location.getMerchantState());
        transaction.setMerchantLatitude(location.getMerchantLatitude());
        transaction.setMerchantLongitude(location.getMerchantLongitude());
//        String postingDate = dtFmt.format(Calendar.getInstance().getTime());
        Long postingDate = Calendar.getInstance().getTime().getTime();
        transaction.setPostingDate(postingDate);
//        transaction.setTransactionDate(dtFmt.format(getTransactionDate()));
        transaction.setTransactionDate(getTransactionDate().getTime());
        transaction.setFraudFlag(fraud);

        if(fraud) {
            setFraudTransactionData(account, location, transaction);
        }else {
            setTransactionAmount(transaction, account, location);

        }
        return transaction;

    }

    private void setFraudTransactionData(Account account, Location location, Transaction transaction)
            throws Exception {
        Double transactionAmount;
        DecimalFormat df  = new DecimalFormat("0.00");
        Double choice = new Random().doubles(1).findFirst().getAsDouble();
        Bounds bounds = null;
        if(choice < 0.4){
            // large transaction amount for account
            bounds = StreamUtil.getBounds(account.getTrxnMean(), account.getTrxStd());
        } else if( choice < 0.8){
            // large transaction amount for merchant
            bounds = StreamUtil.getBounds( location.getMerchantTrxnMean(), location.getMerchantTrxnStd());
        }else {
            //rogue merchant
            transaction.setMerchantName("ACME Hackers");
            transaction.setMerchantCity("Sopchoppy");
            transaction.setMerchantCityAlias("Sopchoppy");
            transaction.setMerchantState("FL");
            transaction.setMerchantLatitude(30.030137);
            transaction.setMerchantLongitude(-84.497957);
            bounds = new Bounds();
            bounds.setLow(5.00);
            bounds.setHigh(10.00);
        }
        Double r = new Random().doubles(1, bounds.getLow(), bounds.getHigh()).findFirst().getAsDouble();
        Double txAmt = r*account.getTrxnMean() *1000;
        transactionAmount = Double.valueOf(df.format(txAmt));
        transaction.setTransactionAmount(Math.abs(transactionAmount));
    }

    static Date getTransactionDate() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -90);
        Date dt1 = cal.getTime();
        Date dt2 = Calendar.getInstance().getTime();
        Date dt= new Date(ThreadLocalRandom.current().nextLong(dt1.getTime(), dt2.getTime()));
        return dt;
    }

    /**
     * Simulate amount of transaction
     * @param transaction
     * @param account
     * @param location
     *
     */
    private void setTransactionAmount(Transaction transaction, Account account, Location location)
     throws Exception {
        double transactionAmount = 0.00;
        DecimalFormat df  = new DecimalFormat("0.00");
        /* Create transaction (account dependent amount) - 20% */
        Double choice = new Random().doubles(1).findFirst().getAsDouble();
        Bounds bounds = null;
        if (choice < 0.2) {
            bounds = StreamUtil.getBounds(account.getTrxnMean(), account.getTrxStd());
        } else {
            //Create transaction (merchant dependent amount) - 80%
            bounds = StreamUtil.getBounds(location.getMerchantTrxnMean(), location.getMerchantTrxnStd());
        }
        double x = Math.random() * ((bounds.getLow() - bounds.getHigh()) + bounds.getLow());
        transactionAmount = Double.valueOf(df.format(x));
        transaction.setTransactionAmount(Math.abs(transactionAmount));
    }

    private ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }


}

